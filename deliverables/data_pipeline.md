# Data Pipeline Lineage Diagram

This document contains the complete Medallion Data Lineage for the **AI-ready transaction investigation context** pipeline.

## 1. End-to-End Lineage (Mermaid Diagram)

```mermaid
flowchart TD
    classDef bronze fill:#d4a373,stroke:#bc6c25,color:#fff,stroke-width:1px;
    classDef silver fill:#457b9d,stroke:#1d3557,color:#fff,stroke-width:1px;
    classDef gold fill:#e9c46a,stroke:#e76f51,color:#2b2d42,stroke-width:1px;
    classDef gate fill:#2a9d8f,stroke:#264653,color:#fff,stroke-width:2px;
    classDef final fill:#9d4edd,stroke:#5a189a,color:#fff,stroke-width:2px;

    subgraph BRONZE ["1. Bronze Layer (Raw Ingestion)"]
        B_CUST["bronze.customers"]:::bronze
        B_TXN["bronze.transactions"]:::bronze
        B_ACC["bronze.accounts"]:::bronze
        B_CARD["bronze.cards"]:::bronze
        B_MERCH["bronze.merchants"]:::bronze
        B_CASE["bronze.investigation_cases"]:::bronze
        B_OTHER["bronze.* (20+ other tables)"]:::bronze
    end

    subgraph M1_GATE ["2. Bronze Acceptance Gate"]
        V1["validate_m1_bronze<br/>(Schema & Ingestion Checks)"]:::gate
    end

    subgraph SILVER ["3. Silver Layer (Cleaned, Typed & Masked)"]
        S_CUST["silver.customers<br/>(Inline DQ & Quarantine)"]:::silver
        S_TXN["silver.transactions<br/>(Inline DQ & Quarantine)"]:::silver
        S_ACC["silver.accounts"]:::silver
        S_CARD["silver.cards"]:::silver
        S_MERCH["silver.merchants"]:::silver
        S_CASE["silver.investigation_cases"]:::silver
        S_OTHER["silver.* (20+ other tables)"]:::silver
        S_QUAR["silver.quarantine_records"]:::silver
    end

    subgraph M2_GATE ["4. Silver Quality Gate"]
        V2["validate_m2_dq<br/>(Quarantine & DQ Validation)"]:::gate
    end

    subgraph GOLD ["5. Gold Layer (Dimensional Star Schema)"]
        G_DATE["dim_date"]:::gold
        G_MERCH["dim_merchant"]:::gold
        G_CHAN["dim_channel"]:::gold
        G_DISP_R["dim_dispute_reason"]:::gold
        G_CURR["dim_currency"]:::gold
        G_CASE["dim_case"]:::gold
        G_FACT_TXN["fact_case_transaction"]:::gold
        G_FACT_AUTH["fact_authorization_attempt"]:::gold
        G_FACT_DISP["fact_dispute"]:::gold
        G_FACT_CB["fact_chargeback"]:::gold
        G_FACT_ALERT["fact_fraud_alert"]:::gold
        G_FACT_NOTE["fact_investigation_note"]:::gold
        G_FACT_PARTY["fact_case_party_summary"]:::gold
    end

    subgraph M3_GATE ["6. Gold Acceptance Validation"]
        V3["validate_m3_gold<br/>(Contract & Grain Validation)"]:::gate
    end

    subgraph AI_CONTEXT ["7. Final AI Data Product"]
        CTX["investigation_context<br/>(AI-Ready Single Table View)"]:::final
    end

    %% Bronze to M1 Gate
    B_CUST --> V1
    B_TXN --> V1
    B_ACC --> V1
    B_CARD --> V1
    B_MERCH --> V1
    B_CASE --> V1
    B_OTHER --> V1

    %% M1 Gate to Silver
    V1 --> S_CUST
    V1 --> S_TXN
    V1 --> S_ACC
    V1 --> S_CARD
    V1 --> S_MERCH
    V1 --> S_CASE
    V1 --> S_OTHER

    %% Bronze to Silver 1-to-1 Data Flow
    B_CUST --> S_CUST
    B_TXN --> S_TXN
    B_ACC --> S_ACC
    B_CARD --> S_CARD
    B_MERCH --> S_MERCH
    B_CASE --> S_CASE
    B_OTHER --> S_OTHER

    %% Silver Quarantine Output
    S_CUST -.->|Rejects| S_QUAR
    S_TXN -.->|Rejects| S_QUAR

    %% Silver to M2 Gate
    S_CUST --> V2
    S_TXN --> V2
    S_ACC --> V2
    S_CARD --> V2
    S_MERCH --> V2
    S_CASE --> V2
    S_OTHER --> V2
    S_QUAR --> V2

    %% M2 Gate to Gold Dims & Facts
    V2 --> G_DATE
    V2 --> G_MERCH
    V2 --> G_CHAN
    V2 --> G_DISP_R
    V2 --> G_CURR
    V2 --> G_CASE
    V2 --> G_FACT_TXN
    V2 --> G_FACT_AUTH
    V2 --> G_FACT_DISP
    V2 --> G_FACT_CB
    V2 --> G_FACT_ALERT
    V2 --> G_FACT_NOTE
    V2 --> G_FACT_PARTY

    %% Gold to M3 Gate
    G_DATE --> V3
    G_MERCH --> V3
    G_CHAN --> V3
    G_DISP_R --> V3
    G_CURR --> V3
    G_CASE --> V3
    G_FACT_TXN --> V3
    G_FACT_AUTH --> V3
    G_FACT_DISP --> V3
    G_FACT_CB --> V3
    G_FACT_ALERT --> V3
    G_FACT_NOTE --> V3
    G_FACT_PARTY --> V3

    %% M3 Gate to Final Context
    V3 --> CTX
    G_CASE --> CTX
    G_FACT_TXN --> CTX
```

---

## 2. How to Export & Include in Documents

### Option A: Embed Directly in Markdown (GitHub, Azure DevOps, Notion, Obsidian)
Paste the ```mermaid ``` block directly into any markdown documentation file (e.g., `README.md` or `architecture.md`). Most modern tools render standard Mermaid syntax interactively.

### Option B: Export High-Res PNG / SVG / PDF
1. **Online Editor (Instant)**:
   - Copy the Mermaid script above.
   - Open [mermaid.live](https://mermaid.live).
   - Paste the script to preview and download as **PNG**, **SVG**, or **Vector Graphics** for slides and documents.

2. **Mermaid CLI (Automated command)**:
   ```bash
   npx @mermaid-js/mermaid-cli -i pipeline_lineage_diagram.md -o lineage_diagram.png
   ```

### Option C: Databricks Unity Catalog Built-in Lineage
In the Databricks Workspace:
1. Open **Catalog Explorer**.
2. Navigate to your catalog (e.g. `g3_catalog.gold.investigation_context`).
3. Click the **Lineage** tab to see the automatically tracked table-level and column-level lineage graph generated at runtime by Unity Catalog.
