// =============================================================================
// Banking Ontology — Neo4j Schema
// Run once on startup to create constraints, indexes, and seed node labels.
// =============================================================================

// ─── Constraints (unique IDs enforce entity deduplication) ───────────────────

CREATE CONSTRAINT customer_id   IF NOT EXISTS FOR (c:Customer)        REQUIRE c.id IS UNIQUE;
CREATE CONSTRAINT account_id    IF NOT EXISTS FOR (a:Account)         REQUIRE a.id IS UNIQUE;
CREATE CONSTRAINT transaction_id IF NOT EXISTS FOR (t:Transaction)    REQUIRE t.id IS UNIQUE;
CREATE CONSTRAINT card_id       IF NOT EXISTS FOR (card:Card)         REQUIRE card.id IS UNIQUE;
CREATE CONSTRAINT loan_id       IF NOT EXISTS FOR (l:Loan)            REQUIRE l.id IS UNIQUE;
CREATE CONSTRAINT merchant_id   IF NOT EXISTS FOR (m:Merchant)        REQUIRE m.id IS UNIQUE;
CREATE CONSTRAINT product_code  IF NOT EXISTS FOR (p:Product)         REQUIRE p.code IS UNIQUE;
CREATE CONSTRAINT segment_id    IF NOT EXISTS FOR (s:CustomerSegment) REQUIRE s.id IS UNIQUE;
CREATE CONSTRAINT branch_id     IF NOT EXISTS FOR (b:Branch)          REQUIRE b.id IS UNIQUE;
CREATE CONSTRAINT mcc_code      IF NOT EXISTS FOR (mc:MerchantCategory) REQUIRE mc.mcc IS UNIQUE;

// ─── Indexes for frequent traversal patterns ──────────────────────────────────

CREATE INDEX customer_income   IF NOT EXISTS FOR (c:Customer) ON (c.income_band);
CREATE INDEX customer_segment  IF NOT EXISTS FOR (c:Customer) ON (c.risk_rating);
CREATE INDEX account_type      IF NOT EXISTS FOR (a:Account)  ON (a.account_type);
CREATE INDEX account_status    IF NOT EXISTS FOR (a:Account)  ON (a.status);
CREATE INDEX tx_timestamp      IF NOT EXISTS FOR (t:Transaction) ON (t.tx_timestamp);
CREATE INDEX tx_channel        IF NOT EXISTS FOR (t:Transaction) ON (t.channel);
CREATE INDEX tx_mcc            IF NOT EXISTS FOR (t:Transaction) ON (t.merchant_mcc);
CREATE INDEX loan_status       IF NOT EXISTS FOR (l:Loan) ON (l.status);
CREATE INDEX loan_type         IF NOT EXISTS FOR (l:Loan) ON (l.loan_type);
CREATE INDEX card_type         IF NOT EXISTS FOR (card:Card) ON (card.card_type);

// ─── Seed Product nodes ───────────────────────────────────────────────────────

MERGE (p:Product {code: 'SAVINGS_STD'})   SET p += {name: 'Standard Savings',    type: 'ACCOUNT', active: true};
MERGE (p:Product {code: 'SAVINGS_PLUS'})  SET p += {name: 'Savings Plus',         type: 'ACCOUNT', active: true};
MERGE (p:Product {code: 'CURRENT_STD'})   SET p += {name: 'Standard Current',     type: 'ACCOUNT', active: true};
MERGE (p:Product {code: 'CURRENT_BIZ'})   SET p += {name: 'Business Current',     type: 'ACCOUNT', active: true};
MERGE (p:Product {code: 'DEBIT_CLASSIC'}) SET p += {name: 'Classic Debit Card',   type: 'CARD',    active: true};
MERGE (p:Product {code: 'CREDIT_GOLD'})   SET p += {name: 'Gold Credit Card',     type: 'CARD',    active: true};
MERGE (p:Product {code: 'CREDIT_PLAT'})   SET p += {name: 'Platinum Credit Card', type: 'CARD',    active: true};
MERGE (p:Product {code: 'CREDIT_CLASSIC'})SET p += {name: 'Classic Credit Card',  type: 'CARD',    active: true};
MERGE (p:Product {code: 'LOAN_PERSONAL'}) SET p += {name: 'Personal Loan',        type: 'LOAN',    active: true};
MERGE (p:Product {code: 'LOAN_AUTO'})     SET p += {name: 'Auto Finance',          type: 'LOAN',    active: true};
MERGE (p:Product {code: 'LOAN_MORTGAGE'}) SET p += {name: 'Home Mortgage',         type: 'LOAN',    active: true};
MERGE (p:Product {code: 'INS_LIFE'})      SET p += {name: 'Life Insurance',        type: 'INSURANCE', active: true};

// ─── Seed CustomerSegment nodes ───────────────────────────────────────────────

MERGE (s:CustomerSegment {id: 'SEG_MASS'})       SET s.name = 'Mass Market';
MERGE (s:CustomerSegment {id: 'SEG_AFFLUENT'})   SET s.name = 'Affluent';
MERGE (s:CustomerSegment {id: 'SEG_HNW'})        SET s.name = 'High Net Worth';
MERGE (s:CustomerSegment {id: 'SEG_YOUTH'})      SET s.name = 'Youth';
MERGE (s:CustomerSegment {id: 'SEG_DIGITAL'})    SET s.name = 'Digital Native';
MERGE (s:CustomerSegment {id: 'SEG_DORMANT'})    SET s.name = 'Dormant';
MERGE (s:CustomerSegment {id: 'SEG_CHURN_RISK'}) SET s.name = 'Churn Risk';

// ─── Seed MerchantCategory nodes ─────────────────────────────────────────────

MERGE (mc:MerchantCategory {mcc: '5411'}) SET mc += {name: 'Grocery Stores',    group: 'RETAIL'};
MERGE (mc:MerchantCategory {mcc: '5812'}) SET mc += {name: 'Restaurants',        group: 'DINING'};
MERGE (mc:MerchantCategory {mcc: '5999'}) SET mc += {name: 'General Retail',     group: 'RETAIL'};
MERGE (mc:MerchantCategory {mcc: '7011'}) SET mc += {name: 'Hotels/Lodging',     group: 'TRAVEL'};
MERGE (mc:MerchantCategory {mcc: '4112'}) SET mc += {name: 'Passenger Railways', group: 'TRAVEL'};
MERGE (mc:MerchantCategory {mcc: '6011'}) SET mc += {name: 'ATM Cash Advance',   group: 'CASH'};
MERGE (mc:MerchantCategory {mcc: '4900'}) SET mc += {name: 'Utilities',           group: 'UTILITIES'};

// =============================================================================
// Sample: Upsert Customer from clean Kafka event (called by brain ingest job)
// =============================================================================
//
// MERGE (c:Customer {id: $customer_id})
// SET c += {
//   income_band:  $income_band,
//   occupation:   $occupation,
//   city:         $city,
//   risk_rating:  $risk_rating,
//   kyc_status:   $kyc_status,
//   is_active:    $is_active,
//   updated_at:   $updated_at
// }
//
// Sample: Link Customer → Account
// MERGE (c:Customer {id: $customer_id})
// MERGE (a:Account  {id: $account_id})
// SET a += {account_type: $account_type, currency: $currency, status: $status, balance: $balance}
// MERGE (c)-[r:HOLDS]->(a)
// SET r.since = $opened_date
//
// Sample: Link Transaction → Customer + Merchant
// MERGE (t:Transaction {id: $tx_id})
// SET t += {amount: $amount, direction: $direction, channel: $channel,
//           tx_timestamp: datetime($tx_timestamp), status: $status}
// MERGE (c:Customer {id: $customer_id})
// MERGE (c)-[:MADE]->(t)
// WITH t
// MERGE (a:Account {id: $account_id})
// MERGE (t)-[:FROM]->(a)
// WITH t
// FOREACH (mid IN CASE WHEN $merchant_id IS NOT NULL THEN [$merchant_id] ELSE [] END |
//   MERGE (m:Merchant {id: mid})
//   SET m.name = $merchant_name, m.mcc = $merchant_mcc
//   MERGE (t)-[:AT]->(m)
// )
