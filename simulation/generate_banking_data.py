"""
Banking Data Generator
Generates realistic UAE-banking simulation data with PII fields.
Output: simulation/data/{customers,accounts,transactions,cards,loans}.json

Run: python generate_banking_data.py
"""

import json
import os
import random
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

random.seed(42)

OUT_DIR = Path(__file__).parent / "data"
OUT_DIR.mkdir(exist_ok=True)

# ── Reference data ─────────────────────────────────────────────────────────────

FIRST_NAMES_MALE   = ["Ahmed", "Mohammed", "Khalid", "Omar", "Saeed", "Hassan",
                       "Ali", "Ibrahim", "Yousuf", "Hamdan", "Rashid", "Sultan",
                       "Tariq", "Faisal", "Majid"]
FIRST_NAMES_FEMALE = ["Fatima", "Aisha", "Mariam", "Sara", "Hessa", "Latifa",
                       "Noura", "Shaikha", "Mahra", "Dana", "Reem", "Layla",
                       "Mona", "Hind", "Nadia"]
LAST_NAMES         = ["Al Rashidi", "Al Mansoori", "Al Zaabi", "Al Mulla",
                       "Al Hammadi", "Al Suwaidi", "Al Shamsi", "Al Nuaimi",
                       "Al Mazrouei", "Al Kaabi", "Al Falasi", "Al Ketbi",
                       "Al Qassimi", "Al Muhairi", "Al Marri"]
CITIES             = ["Dubai", "Abu Dhabi", "Sharjah", "Ajman", "Ras Al Khaimah", "Fujairah"]
OCCUPATIONS        = ["Engineer", "Doctor", "Teacher", "Manager", "Accountant",
                       "Banker", "Lawyer", "Entrepreneur", "Sales Executive", "IT Specialist",
                       "Government Employee", "Nurse", "Architect", "Consultant", "Retired"]
INCOME_BANDS       = ["<5k", "5k-15k", "15k-50k", ">50k"]
INCOME_WEIGHTS     = [0.15, 0.40, 0.35, 0.10]
RISK_RATINGS       = ["LOW", "MEDIUM", "HIGH"]
RISK_WEIGHTS       = [0.60, 0.30, 0.10]
NATIONALITIES      = ["AE", "IN", "PK", "EG", "PH", "GB", "US", "JO", "LB", "BD"]
NAT_WEIGHTS        = [0.35, 0.20, 0.15, 0.10, 0.05, 0.03, 0.03, 0.03, 0.03, 0.03]

BRANCH_IDS = ["BR001", "BR002", "BR003", "BR004", "BR005"]

MERCHANTS = [
    {"merchant_id": "MCH001", "name": "LuLu Hypermarket",   "mcc": "5411", "city": "Dubai"},
    {"merchant_id": "MCH002", "name": "Carrefour UAE",       "mcc": "5411", "city": "Abu Dhabi"},
    {"merchant_id": "MCH003", "name": "ENOC Petrol Station", "mcc": "5541", "city": "Dubai"},
    {"merchant_id": "MCH004", "name": "Al Fardan Exchange",  "mcc": "6099", "city": "Dubai"},
    {"merchant_id": "MCH005", "name": "Emirates NBD ATM",    "mcc": "6011", "city": "Dubai"},
    {"merchant_id": "MCH006", "name": "Noon.com",            "mcc": "5961", "city": "Dubai"},
    {"merchant_id": "MCH007", "name": "Zomato UAE",          "mcc": "5812", "city": "Dubai"},
    {"merchant_id": "MCH008", "name": "DEWA",                "mcc": "4911", "city": "Dubai"},
    {"merchant_id": "MCH009", "name": "Etisalat",            "mcc": "4813", "city": "Abu Dhabi"},
    {"merchant_id": "MCH010", "name": "Dubai Mall",          "mcc": "5999", "city": "Dubai"},
    {"merchant_id": "MCH011", "name": "Emaar",               "mcc": "6552", "city": "Dubai"},
    {"merchant_id": "MCH012", "name": "Talabat",             "mcc": "5812", "city": "Dubai"},
    {"merchant_id": "MCH013", "name": "DP World",            "mcc": "4412", "city": "Dubai"},
    {"merchant_id": "MCH014", "name": "Flydubai",            "mcc": "4511", "city": "Dubai"},
    {"merchant_id": "MCH015", "name": "Gymboree Spa",        "mcc": "7011", "city": "Sharjah"},
]

ACCOUNT_TYPES = ["CURRENT", "SAVINGS", "ISLAMIC_CURRENT", "ISLAMIC_SAVINGS"]
CARD_TYPES    = ["CREDIT_CLASSIC", "CREDIT_GOLD", "CREDIT_PLATINUM", "DEBIT"]
LOAN_TYPES    = ["PERSONAL", "AUTO", "MORTGAGE", "BUSINESS", "EDUCATION"]
TX_CHANNELS   = ["POS", "ONLINE", "ATM", "MOBILE", "BRANCH", "TRANSFER"]
TX_TYPES      = ["PURCHASE", "TRANSFER", "WITHDRAWAL", "DEPOSIT", "BILL_PAYMENT", "REFUND"]


# ── Generators ─────────────────────────────────────────────────────────────────

def rand_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def rand_phone() -> str:
    return f"+9715{random.randint(0,9)}{random.randint(1000000, 9999999)}"


def rand_national_id() -> str:
    return f"784{random.randint(1960,2000)}{random.randint(1000000, 9999999)}"


def rand_iban() -> str:
    return f"AE{random.randint(10,99)}0330000010{random.randint(100000000, 999999999)}"


def generate_customers(n: int = 50) -> list[dict]:
    customers = []
    for i in range(n):
        gender   = random.choice(["M", "F"])
        fn_pool  = FIRST_NAMES_MALE if gender == "M" else FIRST_NAMES_FEMALE
        fname    = random.choice(fn_pool)
        lname    = random.choice(LAST_NAMES)
        dob      = rand_date(date(1960, 1, 1), date(2000, 12, 31))
        nat      = random.choices(NATIONALITIES, weights=NAT_WEIGHTS)[0]
        city     = random.choice(CITIES)
        cid      = f"CUS{str(i+1).zfill(6)}"
        customers.append({
            "customer_id":              cid,
            "national_id":              rand_national_id(),
            "full_name":                f"{fname} {lname}",
            "date_of_birth":            dob.isoformat(),
            "gender":                   gender,
            "nationality":              nat,
            "phone_number":             rand_phone(),
            "email":                    f"{fname.lower()}.{lname.lower().replace(' ','')}{random.randint(1,99)}@email.ae",
            "address_line1":            f"Flat {random.randint(1,30)}, {random.choice(['Marhaba','Al Noor','Garden','Sunrise'])} Tower",
            "address_line2":            f"{random.choice(['Al Barsha','Deira','JVC','Downtown','Jumeirah'])}",
            "city":                     city,
            "country":                  "AE",
            "income_band":              random.choices(INCOME_BANDS, weights=INCOME_WEIGHTS)[0],
            "occupation":               random.choice(OCCUPATIONS),
            "risk_rating":              random.choices(RISK_RATINGS, weights=RISK_WEIGHTS)[0],
            "kyc_status":               "VERIFIED",
            "branch_id":                random.choice(BRANCH_IDS),
            "relationship_manager_id":  f"RM{random.randint(1,10):03d}",
            "is_active":                True,
            "created_at":               rand_date(date(2018, 1, 1), date(2023, 12, 31)).isoformat(),
        })
    return customers


def generate_accounts(customers: list[dict]) -> list[dict]:
    accounts = []
    for c in customers:
        n_accounts = random.choices([1, 2, 3], weights=[0.5, 0.35, 0.15])[0]
        for j in range(n_accounts):
            opened = rand_date(
                date.fromisoformat(c["created_at"]),
                date(2024, 6, 30),
            )
            balance = round(random.uniform(500, 200000), 2)
            accounts.append({
                "account_id":     f"ACC{str(len(accounts)+1).zfill(8)}",
                "customer_id":    c["customer_id"],
                "account_number": rand_iban(),
                "account_type":   random.choice(ACCOUNT_TYPES),
                "currency":       "AED",
                "balance":        balance,
                "available_balance": round(balance * random.uniform(0.8, 1.0), 2),
                "status":         "ACTIVE",
                "branch_id":      c["branch_id"],
                "opened_date":    opened.isoformat(),
                "last_transaction_date": rand_date(date(2024, 1, 1), date(2024, 6, 30)).isoformat(),
            })
    return accounts


def generate_transactions(accounts: list[dict], n: int = 400) -> list[dict]:
    transactions = []
    account_ids  = [a["account_id"] for a in accounts]
    cust_map     = {a["account_id"]: a["customer_id"] for a in accounts}

    for i in range(n):
        acc = random.choice(account_ids)
        merchant = random.choice(MERCHANTS)
        tx_date  = rand_date(date(2024, 1, 1), date(2024, 6, 30))
        tx_type  = random.choice(TX_TYPES)
        amount   = round(random.uniform(5, 15000), 2)

        transactions.append({
            "tx_id":               f"TXN{str(i+1).zfill(9)}",
            "account_id":          acc,
            "customer_id":         cust_map[acc],
            "merchant_id":         merchant["merchant_id"],
            "merchant_name":       merchant["name"],
            "merchant_mcc":        merchant["mcc"],
            "merchant_city":       merchant["city"],
            "amount":              amount,
            "currency":            "AED",
            "fx_rate":             1.0,
            "base_amount":         amount,
            "tx_type":             tx_type,
            "channel":             random.choice(TX_CHANNELS),
            "status":              random.choices(["COMPLETED", "PENDING", "FAILED"], weights=[0.90, 0.07, 0.03])[0],
            "reference_number":    f"REF{random.randint(100000000, 999999999)}",
            "description":         f"{tx_type.title()} at {merchant['name']}",
            "counterparty_account": rand_iban() if tx_type == "TRANSFER" else None,
            "counterparty_name":   f"{random.choice(FIRST_NAMES_MALE)} {random.choice(LAST_NAMES)}" if tx_type == "TRANSFER" else None,
            "created_at":          datetime.combine(tx_date, datetime.min.time()).isoformat() + "Z",
            "value_date":          tx_date.isoformat(),
        })
    return transactions


def generate_cards(customers: list[dict], accounts: list[dict]) -> list[dict]:
    cards   = []
    acc_map = {a["customer_id"]: a["account_id"] for a in accounts}
    for c in random.sample(customers, min(len(customers), 40)):
        issued = rand_date(date(2021, 1, 1), date(2024, 1, 1))
        expiry = issued.replace(year=issued.year + 3)
        cards.append({
            "card_id":         f"CRD{str(len(cards)+1).zfill(7)}",
            "customer_id":     c["customer_id"],
            "account_id":      acc_map.get(c["customer_id"]),
            "card_number":     f"4{random.randint(100000000000000, 999999999999999)}",
            "card_type":       random.choice(CARD_TYPES),
            "cardholder_name": c["full_name"],
            "expiry_date":     expiry.isoformat(),
            "status":          random.choices(["ACTIVE", "INACTIVE", "BLOCKED"], weights=[0.85, 0.10, 0.05])[0],
            "credit_limit":    round(random.uniform(2000, 50000), 2) if "CREDIT" in random.choice(CARD_TYPES) else None,
            "issued_date":     issued.isoformat(),
        })
    return cards


def generate_loans(customers: list[dict]) -> list[dict]:
    loans   = []
    sample  = random.sample(customers, min(len(customers), 25))
    for c in sample:
        disbursed = rand_date(date(2020, 1, 1), date(2024, 1, 1))
        principal = round(random.choice([10000, 25000, 50000, 100000, 250000, 500000, 1000000]), 2)
        term      = random.choice([12, 24, 36, 48, 60, 84, 120])
        loans.append({
            "loan_id":           f"LON{str(len(loans)+1).zfill(7)}",
            "customer_id":       c["customer_id"],
            "loan_type":         random.choice(LOAN_TYPES),
            "principal_amount":  principal,
            "outstanding_amount": round(principal * random.uniform(0.2, 0.95), 2),
            "interest_rate":     round(random.uniform(2.5, 9.5), 2),
            "term_months":       term,
            "monthly_payment":   round(principal / term * random.uniform(1.02, 1.08), 2),
            "status":            random.choices(["ACTIVE", "CLOSED", "DELINQUENT"], weights=[0.70, 0.20, 0.10])[0],
            "disbursed_date":    disbursed.isoformat(),
            "maturity_date":     disbursed.replace(year=disbursed.year + term // 12).isoformat(),
            "collateral_type":   random.choice(["NONE", "VEHICLE", "PROPERTY", "SALARY_ASSIGNMENT"]),
            "purpose":           random.choice(["Home Improvement", "Vehicle Purchase", "Education", "Business", "Personal Use"]),
        })
    return loans


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("Generating banking simulation data…")

    customers    = generate_customers(50)
    accounts     = generate_accounts(customers)
    transactions = generate_transactions(accounts, 400)
    cards        = generate_cards(customers, accounts)
    loans        = generate_loans(customers)

    files = {
        "customers.json":    customers,
        "accounts.json":     accounts,
        "transactions.json": transactions,
        "cards.json":        cards,
        "loans.json":        loans,
        "merchants.json":    MERCHANTS,
    }

    for fname, data in files.items():
        path = OUT_DIR / fname
        path.write_text(json.dumps(data, indent=2, default=str))
        print(f"  {fname:30s} → {len(data):4d} records")

    print(f"\nData saved to {OUT_DIR}")


if __name__ == "__main__":
    main()
