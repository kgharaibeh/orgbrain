# OrgBrain — Banking Vertical: Detailed Architecture

## Entity Model (Ontology Nodes)

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        BANKING ENTITY GRAPH                             │
│                                                                         │
│  (:Branch)──[:EMPLOYS]──►(:Employee)                                    │
│      │                        │                                         │
│      │                   [:MANAGES]                                      │
│      │                        ▼                                         │
│      └──[:SERVES]──────►(:Customer)◄──[:BELONGS_TO]──(:CustomerSegment) │
│                               │                                         │
│              ┌────────────────┼────────────────────────┐                │
│              ▼                ▼                        ▼                │
│         (:Account)       (:Card)                  (:Loan)               │
│         type: [          type: [                  type: [               │
│          CURRENT,         DEBIT,                   PERSONAL,            │
│          SAVINGS,         CREDIT,                  MORTGAGE,            │
│          BUSINESS]        PREPAID]                 AUTO]                │
│              │                │                        │                │
│              │           [:LINKED_TO]            [:COLLATERAL]          │
│              │                │                        ▼                │
│              ▼                ▼                  (:Property)            │
│         (:Transaction)◄──[:CHARGED_TO]                                  │
│              │                                                          │
│    ┌─────────┼──────────────────────────────┐                           │
│    ▼         ▼                              ▼                           │
│ (:Merchant)  (:TransactionCategory)    (:Channel)                       │
│ [MCC code,   [GROCERIES, TRAVEL,        [ATM, BRANCH,                   │
│  name,        DINING, UTILITIES,         MOBILE_APP,                    │
│  location]    ENTERTAINMENT...]          ONLINE, POS]                   │
│                                                                         │
│  (:Product)──[:SUBSCRIBED_TO]──►(:Customer)                             │
│  [SAVINGS_ACCT, CREDIT_CARD,                                            │
│   PERSONAL_LOAN, INSURANCE,                                             │
│   INVESTMENT_FUND]                                                      │
└─────────────────────────────────────────────────────────────────────────┘
```

## Full Data Flow — Banking

```
╔═══════════════════════════════════════════════════════════════════════════╗
║                        BANKING SOURCE SYSTEMS                             ║
║                                                                           ║
║  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ ║
║  │ Core Banking │  │  Card Switch │  │  CRM System  │  │  Loan Mgmt   │ ║
║  │  (PostgreSQL)│  │  (Oracle)    │  │  (MongoDB)   │  │  (MySQL)     │ ║
║  │              │  │              │  │              │  │              │ ║
║  │ • customers  │  │ • card_txns  │  │ • profiles   │  │ • loans      │ ║
║  │ • accounts   │  │ • card_auth  │  │ • segments   │  │ • repayments │ ║
║  │ • txns       │  │ • disputes   │  │ • campaigns  │  │ • collateral │ ║
║  │ • balances   │  │ • limits     │  │ • complaints │  │ • risk_score │ ║
║  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘ ║
╚═════════╪═════════════════╪═════════════════╪═════════════════╪═════════╝
          │ Debezium CDC    │ Debezium CDC    │ Debezium CDC    │ Debezium
          │ (JDBC connector)│ (JDBC connector)│ (Mongo conn.)   │ (JDBC conn.)
          ▼                 ▼                 ▼                 ▼
╔═══════════════════════════════════════════════════════════════════════════╗
║                    KAFKA CLUSTER  (KRaft mode, 3 brokers)                 ║
║                    Apicurio Schema Registry  (Avro schemas)               ║
║                                                                           ║
║  RAW TOPICS (7-day retention):                                            ║
║  ┌──────────────────────────────────────────────────────────────────┐    ║
║  │ raw.core_banking.customers    raw.core_banking.accounts          │    ║
║  │ raw.core_banking.transactions raw.card_switch.transactions       │    ║
║  │ raw.crm.profiles              raw.crm.segments                   │    ║
║  │ raw.loans.applications        raw.loans.repayments               │    ║
║  └──────────────────────────────────────────────────────────────────┘    ║
╚═══════════════════════╤═══════════════════════════════════════════════════╝
                        │
╔═══════════════════════╧═══════════════════════════════════════════════════╗
║              GOVERNANCE MEMBRANE  (Apache Flink Job Cluster)              ║
║                                                                           ║
║  PII Fields detected & anonymized per source:                             ║
║                                                                           ║
║  core_banking.customers:                                                  ║
║   national_id      → FPE numeric  (12345678901234 → 98712345678234)       ║
║   full_name        → HMAC-SHA256  (one-way, consistent token)             ║
║   date_of_birth    → FPE date     (1985-03-22 → 1972-11-09)               ║
║   gender           → generalize   (M/F/Other → PERSON)                   ║
║   phone_number     → FPE numeric  (format preserved)                      ║
║   email            → FPE email    (user@bank.com → xk29@bank.com)         ║
║   address          → suppress     (removed entirely)                      ║
║                                                                           ║
║  core_banking.transactions:                                                ║
║   customer_id      → FPE numeric  (same key → same token, joinable)       ║
║   account_number   → FPE numeric  (format preserved)                      ║
║   counterparty_name→ HMAC-SHA256                                           ║
║   description      → NLP scrub    (Presidio scans free text)              ║
║                                                                           ║
║  card_switch.transactions:                                                 ║
║   card_number      → FPE numeric  (16-digit → 16-digit token)             ║
║   cardholder_name  → HMAC-SHA256                                           ║
║   merchant_name    → KEEP (not PII, analytical value)                     ║
║   amount           → KEEP                                                  ║
║                                                                           ║
║  crm.profiles:                                                            ║
║   all PII fields   → same FPE rules as customers                          ║
║   income_band      → KEEP (bucketed, not PII)                             ║
║   occupation       → KEEP                                                 ║
║                                                                           ║
║  Audit log → kafka.pii_audit (who accessed what token, when)              ║
║  FPE keys  → HashiCorp Vault (transit secrets engine)                     ║
╚═══════════════════════╤═══════════════════════════════════════════════════╝
                        │
            CLEAN TOPICS (7-year retention, compliance):
            ┌────────────────────────────────────────────────────────┐
            │ clean.customers    clean.accounts   clean.transactions  │
            │ clean.cards        clean.loans      clean.crm_profiles  │
            └──────────────────────┬─────────────────────────────────┘
                                   │
          ┌────────────────────────┼───────────────────────────────┐
          ▼                        ▼                               ▼
╔══════════════════╗    ╔═══════════════════════╗    ╔═════════════════════╗
║  ICEBERG / MINIO ║    ║    GRAPH STORE         ║    ║  VECTOR STORE       ║
║  (Data Lake)     ║    ║   (Neo4j Community     ║    ║  (Qdrant)           ║
║                  ║    ║    or Apache AGE)       ║    ║                     ║
║  Raw archive of  ║    ║                         ║    ║  Embeddings of:     ║
║  all clean events║    ║  Nodes:                 ║    ║  • event narratives ║
║  partitioned by: ║    ║  • Customer (anon token)║    ║  • customer profiles║
║  • source        ║    ║  • Account              ║    ║  • merchant summaries║
║  • date          ║    ║  • Transaction          ║    ║  • product usage    ║
║  • entity_type   ║    ║  • Card                 ║    ║    patterns         ║
║                  ║    ║  • Loan                 ║    ║                     ║
║  Used for:       ║    ║  • Merchant             ║    ║  Collection:        ║
║  • replay        ║    ║  • Product              ║    ║  • banking_events   ║
║  • audit         ║    ║  • Branch               ║    ║  • customer_profiles║
║  • ML training   ║    ║  • CustomerSegment      ║    ║  • merchant_intel   ║
║  • compliance    ║    ║                         ║    ║                     ║
╚══════════════════╝    ║  Edges:                 ║    ╚═════════════════════╝
                        ║  • HOLDS (C→Account)    ║
          ▼             ║  • MADE (C→Transaction) ║           ▼
╔══════════════════╗    ║  • AT (Tx→Merchant)     ║    ╔═════════════════════╗
║  TIMESCALEDB     ║    ║  • USES (C→Card)        ║    ║  OLLAMA             ║
║  (Trends)        ║    ║  • HAS (C→Loan)         ║    ║  (Local LLM)        ║
║                  ║    ║  • IN_SEGMENT (C→Seg)   ║    ║                     ║
║  Hypertables:    ║    ║  • SUBSCRIBED (C→Prod)  ║    ║  Models:            ║
║  • tx_volume_1h  ║    ║  • DISPUTES (C→Tx)      ║    ║  • llama3.1:8b      ║
║  • tx_volume_1d  ║    ║  • REFERRED_BY (C→C)    ║    ║    (reasoning)      ║
║  • spend_by_cat  ║    ╚═══════════════════════╝    ║  • nomic-embed-text ║
║  • churn_signals ║                                  ║    (embeddings)     ║
║  • product_usage ║                                  ╚═════════════════════╝
║  • loan_health   ║
╚══════════════════╝
          │                        │                               │
          └────────────────────────┴───────────────────────────────┘
                                   │
╔══════════════════════════════════╧════════════════════════════════════════╗
║                    ORGANIZATION BRAIN  (LlamaIndex + LangGraph)           ║
║                                                                           ║
║  Ontology Builder (Airflow — nightly):                                    ║
║   • Ingest top-1000 new entities → Ollama → extract relationships         ║
║   • Proposed new edge types → human review queue (simple Flask UI)        ║
║   • Merge approved proposals into graph schema                            ║
║   • Re-embed updated entity profiles                                      ║
║                                                                           ║
║  Live Flink jobs (continuous):                                            ║
║   • Entity upsert stream → Neo4j/AGE                                      ║
║   • Event narration → Qdrant (embed every significant event)              ║
║   • Aggregate rollup → TimescaleDB                                        ║
╚══════════════════════════════════╤════════════════════════════════════════╝
                                   │
╔══════════════════════════════════╧════════════════════════════════════════╗
║                  INSIGHTS & DECISIONS AGENT  (LangGraph)                  ║
║                                                                           ║
║  Agent Tools:                                                             ║
║   • graph_query(cypher/sql)   → Neo4j / Apache AGE                        ║
║   • semantic_search(text, k)  → Qdrant nearest-neighbor                   ║
║   • trend_query(sql)          → TimescaleDB                               ║
║   • lake_query(sql)           → Iceberg via DuckDB                        ║
║   • deanonymize(token, reason)→ Vault (policy-gated, audited)             ║
║                                                                           ║
║  Sample Banking Insight Queries:                                          ║
║                                                                           ║
║  Q: "Which customers are at churn risk this month?"                       ║
║  → trend_query: customers with declining tx frequency last 30d            ║
║  → graph_query: cross-check product holdings count                        ║
║  → Result: segment + recommended retention offer                          ║
║                                                                           ║
║  Q: "What products should we kill?"                                       ║
║  → trend_query: product_usage last 90d sorted by adoption delta           ║
║  → graph_query: which segments hold this product exclusively              ║
║  → semantic_search: "customer complaints about X product"                 ║
║  → Result: ranked list with revenue impact estimate                       ║
║                                                                           ║
║  Q: "Run a credit card upsell campaign for segment A"                     ║
║  → graph_query: customers IN_SEGMENT A without credit card                ║
║  → trend_query: spending_velocity, income_band, avg_balance               ║
║  → Result: scored list of anonymized tokens → CRM executes campaign       ║
║                                                                           ║
║  Q: "Summarize the bank's business health this quarter"                   ║
║  → All four tools → Ollama synthesis → executive narrative                ║
║                                                                           ║
╚═══════════════════════╤══════════════════════════════════════════════════╝
                        │
          ┌─────────────┼──────────────┐
          ▼             ▼              ▼
  ┌──────────────┐ ┌─────────┐ ┌─────────────┐
  │Apache Superset│ │REST API │ │ Alert Engine│
  │ Dashboards   │ │(FastAPI)│ │(Grafana     │
  │ • KPIs       │ │for ext. │ │ alerts →    │
  │ • Trends     │ │systems  │ │ Slack/email)│
  │ • Segments   │ └─────────┘ └─────────────┘
  └──────────────┘
```

## Kafka Topic Schema (Avro) — core_banking.transactions

```json
{
  "type": "record",
  "name": "Transaction",
  "namespace": "com.orgbrain.banking",
  "fields": [
    {"name": "tx_id",           "type": "string"},
    {"name": "customer_id",     "type": "string",  "pii": "fpe_numeric"},
    {"name": "account_number",  "type": "string",  "pii": "fpe_numeric"},
    {"name": "card_token",      "type": ["null","string"], "pii": "fpe_numeric"},
    {"name": "amount",          "type": "double"},
    {"name": "currency",        "type": "string"},
    {"name": "direction",       "type": {"type":"enum","name":"Direction","symbols":["DEBIT","CREDIT"]}},
    {"name": "channel",         "type": "string"},
    {"name": "merchant_id",     "type": ["null","string"]},
    {"name": "merchant_name",   "type": ["null","string"]},
    {"name": "merchant_category_code", "type": ["null","string"]},
    {"name": "counterparty_account",   "type": ["null","string"], "pii": "fpe_numeric"},
    {"name": "description",     "type": ["null","string"],        "pii": "nlp_scrub"},
    {"name": "balance_after",   "type": ["null","double"]},
    {"name": "tx_timestamp",    "type": {"type":"long","logicalType":"timestamp-millis"}},
    {"name": "status",          "type": "string"},
    {"name": "_cdc_op",         "type": "string"},
    {"name": "_source_ts",      "type": "long"}
  ]
}
```

## PII Anonymization Decision Table

| Field                | Source          | Method         | Reversible | Joinable | Reason                          |
|---------------------|-----------------|----------------|-----------|----------|---------------------------------|
| national_id          | customers       | FPE numeric    | Yes       | Yes      | Needed for cross-system joins   |
| full_name            | customers       | HMAC-SHA256    | No        | Yes      | Hash consistent across systems  |
| date_of_birth        | customers       | FPE date       | Yes       | No       | Age range preserved, not exact  |
| phone_number         | customers       | FPE numeric    | Yes       | Yes      | SMS campaigns via CRM           |
| email                | customers       | FPE email      | Yes       | Yes      | Email campaigns via CRM         |
| address              | customers       | Suppress       | N/A       | No       | No analytical value             |
| gender               | customers       | Generalize     | No        | No       | k-anonymity, demographics only  |
| account_number       | transactions    | FPE numeric    | Yes       | Yes      | Graph edge Customer→Account     |
| card_number          | card_switch     | FPE numeric    | Yes       | Yes      | Graph edge Customer→Card        |
| cardholder_name      | card_switch     | HMAC-SHA256    | No        | Yes      | Match with customers table      |
| tx_description       | transactions    | NLP scrub      | No        | No       | Free text may embed PII         |
| ip_address           | digital_banking | FPE numeric    | Yes       | No       | Fraud patterns, not attribution |
| device_fingerprint   | digital_banking | HMAC-SHA256    | No        | Yes      | Device clustering               |
```
