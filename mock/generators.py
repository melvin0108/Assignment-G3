"""Per-table row generators with intentional DQ defect injection.

Design:
- Parents generated first; their IDs are stored in `ctx.pools`/`ctx.ids` so
  children produce referentially-clean foreign keys by default.
- Each generator then injects the documented defects (data-model.md §3/§9) at a
  rate derived from `ctx.defect_rate`, logging every injected bad record to
  `ctx.manifest` for downstream quarantine validation.
- Large facts (`transactions`, `auth_attempts`, `transaction_devices`,
  `fraud_alerts`) are generator functions (lazy) so millions of rows stream to
  disk without being held in memory.

Every generator has signature `gen_<table>(ctx, n)` and yields/returns dict rows
whose keys match `config.TABLE_SCHEMAS[<table>]`.
"""
from collections import deque
from dataclasses import dataclass, field
from datetime import timedelta

from . import config as C
from .helpers import (
    iso, iso_date, run_now, seq_id, full_pan, tax_id, phone_au,
    amount, past_ts, future_ts,
)


@dataclass
class Ctx:
    f: object                      # Faker
    rng: object                    # random.Random
    manifest: object               # DefectManifest
    defect_rate: float = 0.05
    counts: dict = field(default_factory=dict)
    ids: dict = field(default_factory=dict)     # table -> list of all ids
    pools: dict = field(default_factory=dict)   # cross-table lookups

    # ---- small helpers ----
    def defect_count(self, n, weight=1.0, min_count=1):
        """Number of defect rows to inject for a population of n."""
        if n <= 0 or self.defect_rate <= 0:
            return 0
        return max(min_count, int(round(n * self.defect_rate * weight)))

    def sample_indices(self, n, k):
        """k distinct row indices in [0, n) for defect placement (deterministic)."""
        k = min(k, n)
        return self.rng.sample(range(n), k) if k > 0 else []


# ===========================================================================
# Reference / lookup tables (clean)
# ===========================================================================
def gen_merchant_categories(ctx, n=None):
    return [{"mcc": m, "category_name": cn, "category_group": g} for m, cn, g in C.MERCHANT_CATEGORIES]


def gen_channels(ctx, n=None):
    return [{"channel_code": c, "channel_name": cn} for c, cn in C.CHANNELS]


def gen_case_status_types(ctx, n=None):
    return [{"status_code": s, "description": d} for s, d in C.CASE_STATUS_TYPES]


def gen_dispute_reason_codes(ctx, n=None):
    return [{"reason_code": r, "description": d} for r, d in C.DISPUTE_REASON_CODES]


def gen_fraud_types(ctx, n=None):
    return [{"fraud_type_code": c, "description": d, "severity": s} for c, d, s in C.FRAUD_TYPES]


def gen_countries(ctx, n=None):
    return [{"iso_code": i, "name": nm, "region": r} for i, nm, r in C.COUNTRIES]


def gen_currencies(ctx, n=None):
    return [{"currency_code": c, "name": nm, "decimals": d} for c, nm, d in C.CURRENCIES]


def gen_branches(ctx, n=None):
    return [{"branch_code": b, "name": nm, "country": co, "region": rg, "status": st}
            for b, nm, co, rg, st in C.BRANCHES]


def gen_date_dim(ctx, n=None):
    from datetime import date, timedelta as td
    start, end = C.DATE_DIM_RANGE
    rows = []
    d = start
    while d <= end:
        rows.append({
            "date_id": d.strftime("%Y%m%d"),
            "year": d.year,
            "month": d.month,
            "quarter": (d.month - 1) // 3 + 1,
            "is_weekend": str(d.weekday() >= 5).lower(),
        })
        d += td(days=1)
    return rows


# ===========================================================================
# Customers
# ===========================================================================
def gen_customers(ctx, n):
    f, rng, man = ctx.f, ctx.rng, ctx.manifest
    now = run_now()
    rows = []
    for i in range(1, n + 1):
        cid = seq_id(C.PFX["customer"], i)
        first, last = f.first_name(), f.last_name()
        rows.append({
            "customer_id": cid,
            "first_name": first, "last_name": last,
            "dob": iso_date(f.date_of_birth(minimum_age=18, maximum_age=85)),
            "email": f.email(),
            "phone": phone_au(rng),
            "address": f"{f.street_address()}, {f.city()}",
            "tax_id": tax_id(rng),
            "created_at": iso(past_ts(rng, now, 1000)),
        })
    ctx.ids["customers"] = [r["customer_id"] for r in rows]
    ctx.pools["cust_email"] = {r["customer_id"]: r["email"] for r in rows}
    ctx.pools["cust_phone"] = {r["customer_id"]: r["phone"] for r in rows}
    ctx.pools["cust_name"] = {r["customer_id"]: f'{r["first_name"]} {r["last_name"]}' for r in rows}

    # --- defects ---
    # 1) missing email
    for idx in ctx.sample_indices(n, ctx.defect_count(n, 0.5)):
        rows[idx]["email"] = ""
        man.add("customers", rows[idx]["customer_id"], "DQ-CUST-EMAIL-FMT",
                "email must match pattern if present", "email is empty")

    # 2) exact duplicate customer_id (clone an existing row)
    next_extra = n + 1000
    for _ in range(ctx.defect_count(n, 0.3)):
        src = rng.choice(rows[:max(1, n // 2)])
        dup = dict(src)
        rows.append(dup)
        man.add("customers", src["customer_id"], "DQ-CUST-ID-DUP",
                "customer_id must be unique", "exact duplicate customer_id")

    # 3) near-duplicate (same name+dob+address+tax_id, new id)
    for j in range(ctx.defect_count(n, 0.3)):
        src = rng.choice(rows[:max(1, n // 2)])
        nid = seq_id(C.PFX["customer"], next_extra + j)
        dup = dict(src)
        dup["customer_id"] = nid
        rows.append(dup)
        man.add("customers", nid, "DQ-CUST-NEAR-DUP",
                "no two customers share name+dob+address+tax_id",
                f"near-duplicate of {src['customer_id']}")

    return rows


# ===========================================================================
# Employees
# ===========================================================================
def gen_employees(ctx, n):
    f, rng, man = ctx.f, ctx.rng, ctx.manifest
    rows = []
    for i in range(1, n + 1):
        name = f"{f.first_name()} {f.last_name()}"
        rows.append({
            "employee_id": seq_id(C.PFX["employee"], i),
            "full_name": name,
            "email": f"{name.lower().replace(' ', '.')}@nab-mock.dev",
            "team": rng.choice(C.EMP_TEAM),
            "role": rng.choice(C.EMP_ROLE),
        })
    ctx.ids["employees"] = [r["employee_id"] for r in rows]

    # --- defects: duplicate email + near-duplicate name (clone employee #1) ---
    if n >= 2:
        base = rows[0]
        dup = {
            "employee_id": seq_id(C.PFX["employee"], n + 1),
            "full_name": base["full_name"],      # near-dup name
            "email": base["email"],              # duplicate email
            "team": rng.choice(C.EMP_TEAM),
            "role": rng.choice(C.EMP_ROLE),
        }
        rows.append(dup)
        man.add("employees", dup["employee_id"], "DQ-EMP-EMAIL-UNIQ",
                "email must be unique", f"duplicate email {base['email']}")
        man.add("employees", dup["employee_id"], "DQ-EMP-NAME-NEAR-DUP",
                "flag near-duplicate employee names", f"near-duplicate of {base['employee_id']}")
    return rows


# ===========================================================================
# Accounts
# ===========================================================================
def gen_accounts(ctx, n):
    rng, man = ctx.rng, ctx.manifest
    cust_ids = ctx.ids["customers"]
    now = run_now()
    rows = []
    for i in range(1, n + 1):
        opened = past_ts(rng, now, 2000)
        rows.append({
            "account_id": seq_id(C.PFX["account"], i),
            "customer_id": rng.choice(cust_ids),
            "product_type": rng.choice(C.ACCOUNT_PRODUCT),
            "open_date": iso_date(opened),
            "status": rng.choices(C.ACCOUNT_STATUS, weights=[80, 8, 8, 4])[0],
            "currency": "AUD",
        })
    ctx.ids["accounts"] = [r["account_id"] for r in rows]

    # --- defects: orphan customer_id + future open_date ---
    for idx in ctx.sample_indices(n, ctx.defect_count(n, 0.5)):
        rows[idx]["customer_id"] = C.ORPHAN_CUSTOMER_ID
        man.add("accounts", rows[idx]["account_id"], "DQ-ACC-CUST-FK",
                "customer_id must exist in customers", "orphan customer_id")
    for idx in ctx.sample_indices(n, ctx.defect_count(n, 0.3)):
        rows[idx]["open_date"] = iso_date((now + timedelta(days=rng.randint(1, 300))).date())
        man.add("accounts", rows[idx]["account_id"], "DQ-ACC-OPENDATE-FUTURE",
                "open_date must not be in the future", "future open_date")
    return rows


# ===========================================================================
# Cards
# ===========================================================================
def gen_cards(ctx, n):
    rng, man = ctx.rng, ctx.manifest
    acct_ids = ctx.ids["accounts"]
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "card_id": seq_id(C.PFX["card"], i),
            "account_id": rng.choice(acct_ids),
            "card_type": rng.choice(C.CARD_TYPE),
            "pan": full_pan(rng),
            "expiry": f"{rng.randint(2024, 2030)}-{rng.randint(1, 12):02d}",
            "status": rng.choices(C.CARD_STATUS, weights=[80, 5, 5, 10])[0],
        })
    ctx.ids["cards"] = [r["card_id"] for r in rows]
    # ensure at least one closed card exists (precondition for transactions defect)
    closed_cards = [r["card_id"] for r in rows if r["status"] == "closed"]
    if not closed_cards:
        rows[0]["status"] = "closed"
        closed_cards = [rows[0]["card_id"]]
    ctx.pools["closed_cards"] = closed_cards

    # --- defects: expired-but-active, duplicate card ---
    for idx in ctx.sample_indices(n, ctx.defect_count(n, 0.4)):
        rows[idx]["expiry"] = "2024-02"
        rows[idx]["status"] = "active"
        man.add("cards", rows[idx]["card_id"], "DQ-CARD-EXPIRED-ACTIVE",
                "active card must not have a past expiry", "expired-but-active")
    for _ in range(ctx.defect_count(n, 0.3)):
        src = rng.choice(rows[:max(1, n // 2)])
        rows.append(dict(src))
        man.add("cards", src["card_id"], "DQ-CARD-DUP",
                "card_id must be unique", "duplicate card")
    return rows


# ===========================================================================
# Merchants
# ===========================================================================
def gen_merchants(ctx, n):
    f, rng, man = ctx.f, ctx.rng, ctx.manifest
    mccs = [m[0] for m in C.MERCHANT_CATEGORIES]
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "merchant_id": seq_id(C.PFX["merchant"], i),
            "name": f.company(),
            "mcc": rng.choice(mccs),
            "country": rng.choice([c[0] for c in C.COUNTRIES]),
            "risk_rating": rng.choice(C.RISK_RATING),
            "status": rng.choices(C.MERCHANT_STATUS, weights=[85, 8, 7])[0],
        })
    ctx.ids["merchants"] = [r["merchant_id"] for r in rows]
    # ensure at least one closed merchant exists (referenced by transactions defect)
    closed = [r["merchant_id"] for r in rows if r["status"] == "closed"]
    if not closed:
        rows[0]["status"] = "closed"
        closed = [rows[0]["merchant_id"]]
    ctx.pools["closed_merchants"] = closed

    # --- defect: inconsistent risk_rating casing ---
    variants = C.INVALID["risk_casing_variants"]
    for idx in ctx.sample_indices(n, ctx.defect_count(n, 0.8, min_count=2)):
        rows[idx]["risk_rating"] = rng.choice(variants)
        man.add("merchants", rows[idx]["merchant_id"], "DQ-MERCH-RISK-CASING",
                "risk_rating must be in {low,medium,high}", "inconsistent casing")
    return rows


# ===========================================================================
# Transactions  (STREAMING — stress target)
# ===========================================================================
TXN_SAMPLE_CAP = 200_000  # cap memory: keep a sample of txn ids/ts for children


def gen_transactions(ctx, n):
    """Yield transaction rows. Collects a downsampled id/ts pool for children."""
    f, rng, man = ctx.f, ctx.rng, ctx.manifest
    acct_ids = ctx.ids["accounts"]
    card_ids = ctx.ids["cards"]
    merch_ids = ctx.ids["merchants"]
    closed_cards = ctx.pools.get("closed_cards", [])
    closed_merchants = ctx.pools.get("closed_merchants", [])
    now = run_now()
    width = max(6, len(str(n)))

    # Pre-pick defect row indices (deterministic).
    defects = {
        "DUP": set(ctx.sample_indices(n, ctx.defect_count(n, 0.4))),
        "NEG": set(ctx.sample_indices(n, ctx.defect_count(n, 0.5))),
        "MISS_MERCH": set(ctx.sample_indices(n, ctx.defect_count(n, 0.4))),
        "ORPHAN_FK": set(ctx.sample_indices(n, ctx.defect_count(n, 0.4))),
        "FUTURE_TS": set(ctx.sample_indices(n, ctx.defect_count(n, 0.4))),
        "CLOSED_CARD": set(ctx.sample_indices(n, ctx.defect_count(n, 0.3))),
    }
    clean_recent = deque(maxlen=200)  # clean rows only, for duplicate injection
    duped_sources = set()             # source ids already duplicated (1 dup each)
    sample_step = max(1, n // TXN_SAMPLE_CAP) if n > TXN_SAMPLE_CAP else 1
    sampled_ids, sampled_ts = [], {}

    i = 0
    produced = 0
    while produced < n:
        i += 1
        tid = seq_id(C.PFX["transaction"], i, width)
        acct = rng.choice(acct_ids)
        card = rng.choice(card_ids)
        merch = rng.choice(merch_ids)
        ts = past_ts(rng, now, 30)
        row = {
            "transaction_id": tid,
            "account_id": acct, "card_id": card, "merchant_id": merch,
            "channel": rng.choice(C.CHANNEL),
            "amount": amount(rng),
            "currency": rng.choices(C.CURRENCY, weights=[88, 5, 3, 2, 2])[0],
            "txn_ts": iso(ts),
            "status": rng.choices(C.TRANSACTION_STATUS, weights=[10, 80, 7, 2, 1])[0],
        }
        idx = produced

        if idx in defects["NEG"]:
            row["amount"] = f"-{amount(rng)}"
            man.add("transactions", tid, "DQ-TXN-AMT-POS", "amount must be > 0", "negative amount")
        if idx in defects["MISS_MERCH"]:
            row["merchant_id"] = ""
            man.add("transactions", tid, "DQ-TXN-MERCH-REQ", "merchant_id is required", "missing merchant_id")
        if idx in defects["ORPHAN_FK"]:
            row["account_id"] = "ACC-9999"
            row["card_id"] = "CARD-9999"
            man.add("transactions", tid, "DQ-TXN-ACCT-FK", "account_id must exist in accounts", "orphan account+card")
        if idx in defects["FUTURE_TS"]:
            row["txn_ts"] = iso(future_ts(rng, now, 365))
            man.add("transactions", tid, "DQ-TXN-TS-FUTURE", "txn_ts must not be in the future", "future timestamp")
        if idx in defects["CLOSED_CARD"] and closed_cards:
            row["card_id"] = rng.choice(closed_cards)
            man.add("transactions", tid, "DQ-TXN-CARD-ACTIVE", "transaction must use an active card", "uses closed card")
        # closed-merchant reference is a realistic data state, not a row-level defect,
        # but we still link some txns to closed merchants for RI-style business checks.
        closed_merch = False
        if closed_merchants and rng.random() < 0.02:
            row["merchant_id"] = rng.choice(closed_merchants)
            closed_merch = True

        yield row
        # keep a pool of clean rows so duplicate clones never inherit another defect
        # (keeps the defect manifest exactly authoritative)
        is_defective = any(idx in s for s in defects.values())
        if not is_defective and not closed_merch:
            clean_recent.append(row)
        if produced % sample_step == 0:
            sampled_ids.append(tid)
            sampled_ts[tid] = row["txn_ts"]
        produced += 1

        if idx in defects["DUP"] and clean_recent:
            candidates = [r for r in clean_recent if r["transaction_id"] not in duped_sources]
            if candidates:
                src = rng.choice(candidates)
                duped_sources.add(src["transaction_id"])
                yield dict(src)
                man.add("transactions", src["transaction_id"], "DQ-TXN-ID-DUP",
                        "transaction_id must be unique", "duplicate transaction_id")

    ctx.pools["txn_sample"] = sampled_ids
    ctx.pools["txn_ts"] = sampled_ts


# ===========================================================================
# Auth attempts  (STREAMING — ~1.2x transactions)
# ===========================================================================
def gen_auth_attempts(ctx, n):
    rng, man = ctx.rng, ctx.manifest
    txn_ids = ctx.pools["txn_sample"]
    now = run_now()
    width = max(6, len(str(n)))
    orphan_set = set(ctx.sample_indices(n, ctx.defect_count(n, 0.3)))
    bad_ts_set = set(ctx.sample_indices(n, ctx.defect_count(n, 0.3)))
    for i in range(1, n + 1):
        tid = rng.choice(txn_ids) if txn_ids else "TXN-999999"
        decision = rng.choices(C.AUTH_DECISION, weights=[88, 12])[0]
        row = {
            "attempt_id": seq_id(C.PFX["attempt"], i, width),
            "transaction_id": tid,
            "decision": decision,
            "decline_reason": "" if decision == "approved" else rng.choice(C.DECLINE_REASON),
            "auth_ts": ctx.pools["txn_ts"].get(tid, iso(past_ts(rng, now, 30))),
        }
        idx = i - 1
        if idx in orphan_set:
            row["transaction_id"] = "TXN-999999"
            man.add("auth_attempts", row["attempt_id"], "DQ-AUTH-TXN-FK",
                    "transaction_id must exist in transactions", "orphan transaction_id")
        if idx in bad_ts_set:
            row["auth_ts"] = iso(future_ts(rng, now, 30))
            man.add("auth_attempts", row["attempt_id"], "DQ-AUTH-TS-ORDER",
                    "auth_ts must not be later than txn_ts", "auth after transaction")
        yield row


# ===========================================================================
# Transaction devices  (STREAMING — ~0.8x transactions)
# ===========================================================================
def gen_transaction_devices(ctx, n):
    f, rng, man = ctx.f, ctx.rng, ctx.manifest
    txn_ids = ctx.pools["txn_sample"]
    now = run_now()
    width = max(6, len(str(n)))
    orphan_set = set(ctx.sample_indices(n, ctx.defect_count(n, 0.3)))
    null_device_set = set(ctx.sample_indices(n, ctx.defect_count(n, 0.2)))
    for i in range(1, n + 1):
        tid = rng.choice(txn_ids) if txn_ids else "TXN-999999"
        row = {
            "device_id": seq_id(C.PFX["device"], i, width),
            "transaction_id": tid,
            "device_type": rng.choice(C.DEVICE_TYPE),
            "ip": f.ipv4_public(),
            "geo_country": rng.choice([c[0] for c in C.COUNTRIES]),
        }
        idx = i - 1
        if idx in orphan_set:
            row["transaction_id"] = "TXN-999999"
            man.add("transaction_devices", row["device_id"], "DQ-DEV-TXN-FK",
                    "transaction_id must exist in transactions", "orphan transaction_id")
        if idx in null_device_set:
            row["device_type"] = ""
            man.add("transaction_devices", row["device_id"], "DQ-DEV-TYPE-REQ",
                    "device_type is required", "missing device_type")
        yield row


# ===========================================================================
# Disputes
# ===========================================================================
def gen_disputes(ctx, n):
    rng, man = ctx.rng, ctx.manifest
    txn_ids = ctx.pools["txn_sample"]
    txn_ts = ctx.pools["txn_ts"]
    rows = []
    for i in range(1, n + 1):
        tid = rng.choice(txn_ids) if txn_ids else "TXN-999999"
        raised = past_ts(rng, run_now(), 10)
        # ensure raised_at >= txn_ts where we know it
        if tid in txn_ts:
            from datetime import datetime as _dt
            try:
                tt = _dt.strptime(txn_ts[tid], "%Y-%m-%dT%H:%M:%SZ")
                if raised < tt:
                    raised = tt + timedelta(hours=rng.randint(1, 48))
            except ValueError:
                pass
        rows.append({
            "dispute_id": seq_id(C.PFX["dispute"], i),
            "transaction_id": tid,
            "reason_code": rng.choice(C.DISPUTE_REASON),
            "amount": amount(rng),
            "status": rng.choice(C.DISPUTE_STATUS),
            "raised_at": iso(raised),
        })
    ctx.ids["disputes"] = [r["dispute_id"] for r in rows]

    # --- defects: orphan txn, status casing, missing reason ---
    for idx in ctx.sample_indices(n, ctx.defect_count(n, 0.5)):
        rows[idx]["transaction_id"] = "TXN-999999"
        man.add("disputes", rows[idx]["dispute_id"], "DQ-DISP-TXN-FK",
                "transaction_id must exist in transactions", "orphan transaction_id")
    for idx in ctx.sample_indices(n, ctx.defect_count(n, 0.4)):
        rows[idx]["status"] = C.INVALID["dispute_status_casing"]
        man.add("disputes", rows[idx]["dispute_id"], "DQ-DISP-STATUS-ENUM",
                "status must be lowercase enum value", "status casing 'Open'")
    for idx in ctx.sample_indices(n, ctx.defect_count(n, 0.4)):
        rows[idx]["reason_code"] = ""
        man.add("disputes", rows[idx]["dispute_id"], "DQ-DISP-REASON-REQ",
                "reason_code is required", "missing reason_code")
    return rows


# ===========================================================================
# Chargebacks
# ===========================================================================
def gen_chargebacks(ctx, n):
    rng, man = ctx.rng, ctx.manifest
    disp_ids = ctx.ids.get("disputes", [])
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "chargeback_id": seq_id(C.PFX["chargeback"], i),
            "dispute_id": rng.choice(disp_ids) if disp_ids else C.ORPHAN_DISPUTE_ID,
            "scheme": rng.choice(C.SCHEME),
            "amount": amount(rng),
            "stage": rng.choice(C.CHARGEBACK_STAGE),
            "processed_at": iso(past_ts(rng, run_now(), 20)),
        })
    for idx in ctx.sample_indices(n, ctx.defect_count(n, 0.4)):
        rows[idx]["dispute_id"] = C.ORPHAN_DISPUTE_ID
        man.add("chargebacks", rows[idx]["chargeback_id"], "DQ-CBK-DISP-FK",
                "dispute_id must exist in disputes", "orphan dispute_id")
    return rows


# ===========================================================================
# Fraud alerts
# ===========================================================================
def gen_fraud_alerts(ctx, n):
    rng, man = ctx.rng, ctx.manifest
    txn_ids = ctx.pools["txn_sample"]
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "alert_id": seq_id(C.PFX["alert"], i),
            "transaction_id": rng.choice(txn_ids) if txn_ids else "TXN-999999",
            "rule_name": rng.choice(["velocity_5min", "geo_mismatch", "high_value_night", "new_device_high_value"]),
            "score": f"{round(rng.uniform(0.5, 0.99), 2)}",
            "triggered_at": iso(past_ts(rng, run_now(), 30)),
            "disposition": rng.choice(C.FRAUD_ALERT_DISPOSITION),
        })
    # --- defect: score out of [0,1] ---
    for idx in ctx.sample_indices(n, ctx.defect_count(n, 0.5)):
        rows[idx]["score"] = f"{round(rng.uniform(1.2, 5.0), 2)}"
        man.add("fraud_alerts", rows[idx]["alert_id"], "DQ-ALT-SCORE-RANGE",
                "score must be within [0,1]", "score out of range")
    return rows


# ===========================================================================
# Investigation cases
# ===========================================================================
def gen_investigation_cases(ctx, n):
    rng, man = ctx.rng, ctx.manifest
    emp_ids = ctx.ids["employees"]
    now = run_now()
    rows = []
    for i in range(1, n + 1):
        opened = past_ts(rng, now, 400)
        status = rng.choice(C.CASE_STATUS)
        closed = None
        if status == "closed":
            closed = opened + timedelta(days=rng.randint(1, 60))
        rows.append({
            "case_id": seq_id(C.PFX["case"], i),
            "priority": rng.choice(C.CASE_PRIORITY),
            "status_code": status,
            "fraud_type_code": rng.choice(C.FRAUD_TYPE),
            "owner_employee_id": rng.choice(emp_ids),
            "opened_at": iso(opened),
            "closed_at": iso(closed) if closed else "",
            "legal_hold": "false",
        })
    ctx.ids["cases"] = [r["case_id"] for r in rows]

    # ensure at least one legal_hold case (precondition for notes/contact defects)
    legal_hold_ids = []
    lh_count = max(1, ctx.defect_count(n, 0.2))
    for idx in ctx.sample_indices(n, lh_count):
        rows[idx]["legal_hold"] = "true"
        rows[idx]["status_code"] = "suspended"
        legal_hold_ids.append(rows[idx]["case_id"])
        man.add("investigation_cases", rows[idx]["case_id"], "DQ-CASE-LEGALHOLD",
                "legal_hold cases excluded from AI output", "legal_hold=true (must-not-expose)")
    ctx.pools["legal_hold_cases"] = legal_hold_ids

    # --- defect: status_code not in enum ---
    for idx in ctx.sample_indices(n, ctx.defect_count(n, 0.3)):
        rows[idx]["status_code"] = C.INVALID["case_status_unknown"]
        man.add("investigation_cases", rows[idx]["case_id"], "DQ-CASE-STATUS-ENUM",
                "status_code must be in case_status_types", "status 'on_hold' not in enum")

    # --- defect: stale open case (open > 180 days) ---
    for idx in ctx.sample_indices(n, ctx.defect_count(n, 0.4)):
        rows[idx]["opened_at"] = iso(now - timedelta(days=rng.randint(200, 700)))
        rows[idx]["status_code"] = "open"
        rows[idx]["closed_at"] = ""
        man.add("investigation_cases", rows[idx]["case_id"], "DQ-CASE-STALE",
                "open cases older than 180 days are stale", "stale open case")
    return rows


# ===========================================================================
# Investigation notes
# ===========================================================================
def gen_investigation_notes(ctx, n):
    f, rng, man = ctx.f, ctx.rng, ctx.manifest
    case_ids = ctx.ids["cases"]
    emp_ids = ctx.ids["employees"]
    cust_emails = ctx.pools["cust_email"]
    cust_phones = ctx.pools["cust_phone"]
    cust_names = ctx.pools["cust_name"]
    legal_hold = ctx.pools.get("legal_hold_cases", [])
    now = run_now()
    rows = []
    cust_keys = list(cust_emails.keys())
    for i in range(1, n + 1):
        rows.append({
            "note_id": seq_id(C.PFX["note"], i, 5),
            "case_id": rng.choice(case_ids),
            "author_employee_id": rng.choice(emp_ids),
            "note_text": rng.choice([
                "Reviewed linked transactions, pattern consistent with merchant profile.",
                "Customer confirmed authorised usage. No further action.",
                "Awaiting device fingerprint analysis from security team.",
            ]),
            "created_at": iso(past_ts(rng, now, 30)),
        })

    # --- defect: PII leakage inside note_text ---
    leak_count = ctx.defect_count(n, 0.5, min_count=1)
    for idx in ctx.sample_indices(n, leak_count):
        ck = rng.choice(cust_keys) if cust_keys else None
        leaked = []
        if ck:
            leaked += [f"name '{cust_names[ck]}'", cust_emails[ck], cust_phones[ck]]
        leaked.append(f"card {full_pan(rng)}")
        rows[idx]["note_text"] = (f"Spoke to customer ({', '.join(leaked)}). Card compromised, "
                                  "reissue requested.")
        man.add("investigation_notes", rows[idx]["note_id"], "DQ-NOTE-PII-LEAK",
                "note_text must not contain raw PII/PAN", "leaked PII and PAN in free text")

    # --- defect: note on a legal_hold case ---
    if legal_hold:
        for idx in ctx.sample_indices(n, ctx.defect_count(n, 0.3, min_count=1)):
            rows[idx]["case_id"] = rng.choice(legal_hold)
            rows[idx]["note_text"] = "SAR filed - legal hold. Do not disclose."
            man.add("investigation_notes", rows[idx]["note_id"], "DQ-NOTE-LEGALHOLD",
                    "notes on legal_hold cases must not reach AI", "note on legal_hold case")
    return rows


# ===========================================================================
# Bridges
# ===========================================================================
def gen_case_transactions(ctx, n):
    rng, man = ctx.rng, ctx.manifest
    case_ids = ctx.ids["cases"]
    txn_ids = ctx.pools["txn_sample"]
    now = run_now()
    rows = []
    seen = set()
    i = 0
    while len(rows) < n:
        i += 1
        cid = rng.choice(case_ids)
        tid = rng.choice(txn_ids) if txn_ids else "TXN-999999"
        if (cid, tid) in seen:
            continue
        seen.add((cid, tid))
        rows.append({
            "case_id": cid, "transaction_id": tid,
            "linked_at": iso(past_ts(rng, now, 30)),
        })
    for idx in ctx.sample_indices(n, ctx.defect_count(n, 0.3)):
        rows[idx]["transaction_id"] = "TXN-999999"
        man.add("case_transactions", f'{rows[idx]["case_id"]}|{rows[idx]["transaction_id"]}',
                "DQ-CASETXN-TXN-FK", "transaction_id must exist in transactions", "orphan transaction_id")
    return rows


def gen_case_parties(ctx, n):
    rng, man = ctx.rng, ctx.manifest
    case_ids = ctx.ids["cases"]
    cust_ids = ctx.ids["customers"]
    merch_ids = ctx.ids["merchants"]
    rows = []
    seen = set()
    i = 0
    while len(rows) < n:
        i += 1
        cid = rng.choice(case_ids)
        ptype = rng.choice(C.PARTY_TYPE)
        if ptype == "customer":
            pid = rng.choice(cust_ids)
        elif ptype == "merchant":
            pid = rng.choice(merch_ids)
        else:
            pid = seq_id(C.PFX["third_party"], rng.randint(1, 9999))
        key = (cid, ptype, pid)
        if key in seen:
            continue
        seen.add(key)
        rows.append({
            "case_id": cid, "party_type": ptype, "party_id": pid,
            "role": rng.choice(C.PARTY_ROLE),
        })
    # --- defect: party_id not resolvable for customer/merchant type ---
    for idx in ctx.sample_indices(n, ctx.defect_count(n, 0.4, min_count=1)):
        rows[idx]["party_type"] = "customer"
        rows[idx]["party_id"] = "MCH-9999"  # a merchant-shaped id under customer type
        man.add("case_parties", f'{rows[idx]["case_id"]}|customer|MCH-9999',
                "DQ-CASEPARTY-RESOLVE", "party_id must resolve per party_type",
                "customer party_id not in customers")
    # --- defect: invalid party_type ---
    for idx in ctx.sample_indices(n, ctx.defect_count(n, 0.2, min_count=1)):
        rows[idx]["party_type"] = "suspect"
        man.add("case_parties", f'{rows[idx]["case_id"]}|suspect|{rows[idx]["party_id"]}',
                "DQ-CASEPARTY-TYPE-ENUM", "party_type must be in {customer,merchant,third_party}",
                "invalid party_type")
    return rows


# ===========================================================================
# Customer contact logs
# ===========================================================================
def gen_customer_contact_logs(ctx, n):
    f, rng, man = ctx.f, ctx.rng, ctx.manifest
    cust_ids = ctx.ids["customers"]
    emp_ids = ctx.ids["employees"]
    cust_emails = ctx.pools["cust_email"]
    cust_phones = ctx.pools["cust_phone"]
    cust_names = ctx.pools["cust_name"]
    now = run_now()
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "contact_id": seq_id(C.PFX["contact"], i),
            "customer_id": rng.choice(cust_ids),
            "direction": rng.choice(C.CONTACT_DIRECTION),
            "contact_method": rng.choice(C.CONTACT_METHOD),
            "do_not_contact": "false",
            "contacted_at": iso(past_ts(rng, now, 30)),
            "employee_id": rng.choice(emp_ids),
            "note": "",
        })
    # --- defect: outbound contact to a do_not_contact=true customer ---
    for idx in ctx.sample_indices(n, ctx.defect_count(n, 0.4, min_count=1)):
        rows[idx]["direction"] = "outbound"
        rows[idx]["do_not_contact"] = "true"
        man.add("customer_contact_logs", rows[idx]["contact_id"], "DQ-CTL-DNC-VIOLATION",
                "no outbound contact when do_not_contact=true", "DNC business-rule break")
    # --- defect: note leaking PII ---
    for idx in ctx.sample_indices(n, ctx.defect_count(n, 0.4, min_count=1)):
        ck = rows[idx]["customer_id"]
        leak = []
        if ck in cust_emails:
            leak += [cust_emails[ck], cust_phones[ck]]
        leak.append(f"card {full_pan(rng)}")
        rows[idx]["note"] = (f"Caller confirmed name {cust_names.get(ck, 'customer')} "
                             f"({', '.join(leak)}).")
        man.add("customer_contact_logs", rows[idx]["contact_id"], "DQ-CTL-NOTE-PII",
                "note must not contain raw PII/PAN", "leaked PII in contact note")
    return rows


# ===========================================================================
# Registry
# ===========================================================================
GENERATORS = {
    "merchant_categories": gen_merchant_categories,
    "channels": gen_channels,
    "case_status_types": gen_case_status_types,
    "dispute_reason_codes": gen_dispute_reason_codes,
    "fraud_types": gen_fraud_types,
    "countries": gen_countries,
    "currencies": gen_currencies,
    "branches": gen_branches,
    "date_dim": gen_date_dim,
    "customers": gen_customers,
    "employees": gen_employees,
    "accounts": gen_accounts,
    "cards": gen_cards,
    "merchants": gen_merchants,
    "transactions": gen_transactions,
    "auth_attempts": gen_auth_attempts,
    "transaction_devices": gen_transaction_devices,
    "disputes": gen_disputes,
    "chargebacks": gen_chargebacks,
    "fraud_alerts": gen_fraud_alerts,
    "investigation_cases": gen_investigation_cases,
    "investigation_notes": gen_investigation_notes,
    "case_transactions": gen_case_transactions,
    "case_parties": gen_case_parties,
    "customer_contact_logs": gen_customer_contact_logs,
}
