"""Faker + value helpers shared across generators."""
import random as _random
from datetime import datetime, timedelta

from faker import Faker

from . import config as C


def make_faker(seed: int) -> Faker:
    """Faker instance; seeding Faker.seed makes all calls deterministic."""
    Faker.seed(seed)
    return Faker(["en_AU"])


def make_rng(seed: int) -> _random.Random:
    return _random.Random(seed)


def run_now() -> datetime:
    """Midnight on the pinned RUN_DATE as a datetime."""
    return datetime.combine(C.RUN_DATE, datetime.min.time())


def iso(dt) -> str:
    if dt is None:
        return ""
    if isinstance(dt, str):
        return dt
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def iso_date(d) -> str:
    if d is None:
        return ""
    if isinstance(d, str):
        return d
    return d.strftime("%Y-%m-%d")


def seq_id(prefix: str, i: int, width: int = 4) -> str:
    return f"{prefix}{i:0{width}d}"


def masked_pan(rng: _random.Random) -> str:
    """Properly masked PAN: 12 mask chars + last 4 (data-model §4.3)."""
    return "############" + str(rng.randint(1000, 9999))


def full_pan(rng: _random.Random) -> str:
    """Intentionally UNMASKED PAN (defect — leakage)."""
    return f"{rng.randint(1000,9999)}-{rng.randint(1000,9999)}-{rng.randint(1000,9999)}-{rng.randint(1000,9999)}"


def tax_id(rng: _random.Random) -> str:
    """Synthetic 9-digit tax id (no real TFN)."""
    return str(rng.randint(100_000_000, 999_999_999))


def phone_au(rng: _random.Random) -> str:
    return "+61" + str(rng.randint(400_000_000, 499_999_999))


def amount(rng: _random.Random, low: float = 1.0, high: float = 500.0) -> str:
    return f"{round(rng.uniform(low, high), 2):.2f}"


def past_ts(rng: _random.Random, now: datetime, max_days_back: int) -> datetime:
    return now - timedelta(days=rng.randint(0, max_days_back), seconds=rng.randint(0, 86_399))


def future_ts(rng: _random.Random, now: datetime, max_days_ahead: int = 365) -> datetime:
    return now + timedelta(days=rng.randint(1, max_days_ahead), seconds=rng.randint(0, 86_399))
