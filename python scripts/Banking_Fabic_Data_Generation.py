#!/usr/bin/env python3
"""
Complete Synthetic Multi-Source Banking Dataset Generator
for Fraud & Risk Analytics Project.

Generates ALL tables from scratch with ALL fields (original + extended).
No fraud_flag column. Fraud is derived from behavioral signals only.

Output:
  - transactions.csv   (~1M rows)
  - customers.csv      (~50K rows)
  - devices.csv        (~100K rows)
  - merchants.csv      (~10K rows)
  - accounts.csv       (~60K+ rows)
  - fx_rates.csv       (~2928 rows)
"""

import numpy as np
import pandas as pd
import os
import time
import string
from datetime import datetime

# ============================================================
# CONFIGURATION
# ============================================================
SEED = 42
np.random.seed(SEED)

NUM_CUSTOMERS = 50_000
NUM_TRANSACTIONS = 1_000_000
NUM_MERCHANTS = 10_000
NUM_DEVICES = 100_000

NORMAL_FRAC = 0.90
MEDIUM_ANOMALY_FRAC = 0.05
EXTREME_ANOMALY_FRAC = 0.05

DUPLICATE_TXN_FRAC = 0.0005
NEGATIVE_AMOUNT_FRAC = 0.001
ZERO_AMOUNT_FRAC = 0.001
NULL_DEVICE_FRAC = 0.005
BAD_TIMESTAMP_FRAC = 0.0005

OUTPUT_DIR = "/Users/piyushpanthi/Documents/Data Analytics/Banking Fraud Detection Data Analysis"
os.makedirs(OUTPUT_DIR, exist_ok=True)

START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2025, 12, 31)
DATE_RANGE_SECONDS = int((END_DATE - START_DATE).total_seconds())

# ============================================================
# REFERENCE DATA
# ============================================================
CURRENCIES = ["USD", "GBP", "INR", "EUR"]
CHANNELS = ["POS", "ONLINE", "ATM", "MOBILE"]
TXN_TYPES = ["DEBIT", "CREDIT"]
INCOME_BANDS = ["LOW", "MID", "HIGH", "ULTRA"]
RISK_PROFILES = ["LOW", "MEDIUM", "HIGH"]
CUSTOMER_SEGMENTS = ["Retail", "SME", "Corporate"]
DEVICE_TYPES = ["Mobile", "Desktop", "Tablet"]

COUNTRIES_CLEAN = [
    "US", "GB", "IN", "DE", "FR", "JP", "AU", "CA", "SG", "AE",
    "BR", "MX", "NG", "ZA", "KE", "HK", "NL", "CH", "SE", "IT"
]
COUNTRY_WEIGHTS = np.array([
    0.25, 0.12, 0.15, 0.06, 0.05, 0.04, 0.04, 0.05, 0.03, 0.03,
    0.03, 0.02, 0.02, 0.02, 0.01, 0.02, 0.02, 0.01, 0.01, 0.02
])
COUNTRY_WEIGHTS /= COUNTRY_WEIGHTS.sum()

GB_VARIANTS = ["UK", "United Kingdom", "GB", "Britain", "Great Britain", "U.K."]

MERCHANT_CATEGORIES = [
    "Grocery", "Electronics", "Restaurants", "Travel", "Fuel",
    "Healthcare", "Education", "Entertainment", "Clothing", "Utilities",
    "Gambling", "Crypto", "Offshore Services", "Digital Goods",
    "Insurance", "Real Estate", "Automotive", "Telecom"
]
HIGH_RISK_CATEGORIES = {"Gambling", "Crypto", "Offshore Services", "Digital Goods"}
MERCHANT_RISK_LEVELS = ["LOW", "MEDIUM", "HIGH"]

INCOME_SPEND_MU = {"LOW": 3.0, "MID": 4.5, "HIGH": 6.0, "ULTRA": 7.5}
INCOME_SPEND_SIGMA = {"LOW": 1.0, "MID": 1.0, "HIGH": 1.2, "ULTRA": 1.5}

COUNTRY_DIAL_CODES = {
    "US": "+1", "GB": "+44", "IN": "+91", "DE": "+49", "FR": "+33",
    "JP": "+81", "AU": "+61", "CA": "+1", "SG": "+65", "AE": "+971",
    "BR": "+55", "MX": "+52", "NG": "+234", "ZA": "+27", "KE": "+254",
    "HK": "+852", "NL": "+31", "CH": "+41", "SE": "+46", "IT": "+39"
}
ALL_DIAL_CODES = list(set(COUNTRY_DIAL_CODES.values()))

COUNTRY_CURRENCY_MAP = {
    "US": "USD", "GB": "GBP", "IN": "INR", "DE": "EUR", "FR": "EUR",
    "JP": "JPY", "AU": "AUD", "CA": "CAD", "SG": "SGD", "AE": "AED",
    "BR": "BRL", "MX": "MXN", "NG": "NGN", "ZA": "ZAR", "KE": "KES",
    "HK": "HKD", "NL": "EUR", "CH": "CHF", "SE": "SEK", "IT": "EUR"
}

MCC_MAP = {
    "Gambling": 7995, "Crypto": 6051, "Offshore Services": 6099,
    "Digital Goods": 5815, "Grocery": 5411, "Travel": 4722,
    "Restaurants": 5812, "Electronics": 5732, "Healthcare": 8099,
    "Education": 8220, "Entertainment": 7922, "Utilities": 4900,
    "Insurance": 6311, "Real Estate": 6512, "Automotive": 5511,
    "Telecom": 4813, "Fuel": 5541, "Clothing": 5651
}
MCC_DEFAULT = 5999

FATF_HIGH_RISK = {
    "IR", "KP", "MM", "SY", "YE", "LY", "SD", "SS", "SO", "HT",
    "PK", "AE", "PH"
}

COUNTRY_CITIES = {
    "US": ["New York", "Los Angeles", "Chicago"],
    "GB": ["London", "Manchester", "Birmingham"],
    "IN": ["Mumbai", "Delhi", "Bangalore"],
    "DE": ["Berlin", "Munich", "Hamburg"],
    "FR": ["Paris", "Lyon", "Marseille"],
    "JP": ["Tokyo", "Osaka", "Yokohama"],
    "AU": ["Sydney", "Melbourne", "Brisbane"],
    "CA": ["Toronto", "Vancouver", "Montreal"],
    "SG": ["Singapore", "Jurong East", "Tampines"],
    "AE": ["Dubai", "Abu Dhabi", "Sharjah"],
    "BR": ["São Paulo", "Rio de Janeiro", "Brasília"],
    "MX": ["Mexico City", "Guadalajara", "Monterrey"],
    "NG": ["Lagos", "Abuja", "Kano"],
    "ZA": ["Johannesburg", "Cape Town", "Durban"],
    "KE": ["Nairobi", "Mombasa", "Kisumu"],
    "HK": ["Hong Kong", "Kowloon", "Tsuen Wan"],
    "NL": ["Amsterdam", "Rotterdam", "The Hague"],
    "CH": ["Zurich", "Geneva", "Basel"],
    "SE": ["Stockholm", "Gothenburg", "Malmö"],
    "IT": ["Rome", "Milan", "Naples"]
}


# ============================================================
# UTILITIES
# ============================================================
def timer(func):
    def wrapper(*args, **kwargs):
        t0 = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - t0
        print(f"  [{func.__name__}] completed in {elapsed:.2f}s")
        return result
    return wrapper


def save_csv(df, filename):
    filepath = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(filepath, index=False)
    size_mb = os.path.getsize(filepath) / (1024 * 1024)
    print(f"  ✅ Saved {filepath}")
    print(f"     → {len(df):,} rows | {len(df.columns)} columns | {size_mb:.1f} MB")
    return filepath


def print_validation(df, name, cat_cols=None, num_cols=None):
    print(f"\n  {'─'*60}")
    print(f"  VALIDATION: {name}")
    print(f"  {'─'*60}")
    print(f"  Rows:    {len(df):,}")
    print(f"  Columns: {list(df.columns)}")

    nulls = df.isnull().sum()
    null_cols = nulls[nulls > 0]
    if len(null_cols) > 0:
        print(f"  Nulls:")
        for col, cnt in null_cols.items():
            print(f"    {col:<35} {cnt:>10,} ({cnt/len(df)*100:.3f}%)")

    if cat_cols:
        for col in cat_cols:
            if col in df.columns:
                print(f"  {col} distribution:")
                for val, cnt in df[col].value_counts().head(8).items():
                    print(f"    {str(val):<28} {cnt:>10,} ({cnt/len(df)*100:.2f}%)")

    if num_cols:
        for col in num_cols:
            if col in df.columns:
                s = pd.to_numeric(df[col], errors="coerce")
                print(f"  {col}: min={s.min():.2f} | max={s.max():.2f} | "
                      f"mean={s.mean():.2f} | median={s.median():.2f}")


# ============================================================
# 1. CUSTOMERS (50K) — ALL FIELDS
# ============================================================
@timer
def generate_customers():
    print("\n" + "=" * 70)
    print("GENERATING: customers.csv (ALL FIELDS)")
    print("=" * 70)
    n = NUM_CUSTOMERS
    cust_ids = np.arange(1, n + 1)

    segments = np.random.choice(CUSTOMER_SEGMENTS, size=n, p=[0.70, 0.20, 0.10])
    risk = np.random.choice(RISK_PROFILES, size=n, p=[0.60, 0.25, 0.15])

    income = np.empty(n, dtype=object)
    seg_arr = np.array(segments)
    for seg, probs in [("Retail", [0.40, 0.35, 0.20, 0.05]),
                        ("SME", [0.10, 0.30, 0.40, 0.20]),
                        ("Corporate", [0.05, 0.15, 0.35, 0.45])]:
        mask = seg_arr == seg
        income[mask] = np.random.choice(INCOME_BANDS, size=mask.sum(), p=probs)

    home_country = np.random.choice(COUNTRIES_CLEAN, size=n, p=COUNTRY_WEIGHTS)

    base = np.datetime64("2025-12-31")
    customer_since = base - np.random.randint(0, 3650, size=n).astype("timedelta64[D]")
    dob = base - np.random.randint(18 * 365, 80 * 365, size=n).astype("timedelta64[D]")

    kyc = np.empty(n, dtype=object)
    for rp, probs in [("HIGH", [0.50, 0.20, 0.30]),
                       ("MEDIUM", [0.70, 0.18, 0.12]),
                       ("LOW", [0.95, 0.03, 0.02])]:
        mask = risk == rp
        kyc[mask] = np.random.choice(["VERIFIED", "PENDING", "FAILED"], size=mask.sum(), p=probs)

    credit_limit = np.empty(n, dtype=np.float64)
    for band, (lo, hi) in [("LOW", (500, 2000)), ("MID", (2000, 10000)),
                             ("HIGH", (10000, 50000)), ("ULTRA", (50000, 500000))]:
        mask = income == band
        credit_limit[mask] = np.random.uniform(lo, hi, size=mask.sum())
    credit_limit = np.round(credit_limit, 2)

    current_balance = np.round(np.random.uniform(0, 0.95, size=n) * credit_limit, 2)

    email_domain = np.random.choice(
        ["gmail.com", "yahoo.com", "outlook.com", "corpmail.com", "bizmail.net"],
        size=n, p=[0.45, 0.20, 0.15, 0.10, 0.10]
    )

    phone_codes = np.array([COUNTRY_DIAL_CODES.get(c, "+1") for c in home_country])
    mismatch = np.random.random(n) < 0.15
    phone_codes[mismatch] = np.random.choice(ALL_DIAL_CODES, size=mismatch.sum())

    num_accounts = np.random.choice(
        [1, 2, 3, 4, 5, 6], size=n, p=[0.70, 0.20, 0.08, 0.01, 0.005, 0.005]
    )

    chargeback = np.zeros(n, dtype=np.int32)
    for rp, zero_pct, lam in [("LOW", 0.90, 0.5), ("MEDIUM", 0.70, 1.5), ("HIGH", 0.40, 3.0)]:
        mask = risk == rp
        has_cb = np.random.random(mask.sum()) >= zero_pct
        vals = np.zeros(mask.sum(), dtype=np.int32)
        vals[has_cb] = np.random.poisson(lam, size=has_cb.sum()).astype(np.int32)
        chargeback[mask] = vals

    change_dates = np.full(n, np.datetime64("NaT"), dtype="datetime64[D]")
    has_change = np.random.random(n) < 0.30
    change_dates[has_change] = base - np.random.randint(0, 730, size=has_change.sum()).astype("timedelta64[D]")

    pref_currency = np.array([COUNTRY_CURRENCY_MAP.get(c, "USD") for c in home_country])

    df = pd.DataFrame({
        "customer_id": cust_ids,
        "customer_since": customer_since,
        "home_country": pd.Categorical(home_country),
        "income_band": pd.Categorical(income, categories=INCOME_BANDS),
        "risk_profile": pd.Categorical(risk, categories=RISK_PROFILES),
        "date_of_birth": dob,
        "customer_segment": pd.Categorical(segments, categories=CUSTOMER_SEGMENTS),
        "kyc_status": pd.Categorical(kyc),
        "credit_limit": credit_limit,
        "current_balance": current_balance,
        "email_domain": email_domain,
        "phone_country_code": phone_codes,
        "num_accounts": num_accounts,
        "chargeback_count": chargeback,
        "last_contact_change_date": change_dates,
        "preferred_currency": pref_currency,
    })

    save_csv(df, "customers.csv")
    print_validation(df, "customers.csv",
                     cat_cols=["customer_segment", "risk_profile", "income_band", "kyc_status"],
                     num_cols=["credit_limit", "current_balance", "chargeback_count"])
    return df


# ============================================================
# 2. MERCHANTS (10K) — ALL FIELDS
# ============================================================
@timer
def generate_merchants():
    print("\n" + "=" * 70)
    print("GENERATING: merchants.csv (ALL FIELDS)")
    print("=" * 70)
    n = NUM_MERCHANTS
    merch_ids = np.arange(1, n + 1)

    categories = np.random.choice(MERCHANT_CATEGORIES, size=n)
    countries = np.random.choice(COUNTRIES_CLEAN, size=n, p=COUNTRY_WEIGHTS)

    risk = np.where(
        np.isin(categories, list(HIGH_RISK_CATEGORIES)),
        np.random.choice(["MEDIUM", "HIGH"], size=n, p=[0.3, 0.7]),
        np.random.choice(MERCHANT_RISK_LEVELS, size=n, p=[0.65, 0.25, 0.10])
    )

    mcc = np.array([MCC_MAP.get(cat, MCC_DEFAULT) for cat in categories], dtype=np.int32)

    base = np.datetime64("2024-12-31")
    merch_since = base - np.random.randint(0, 3652, size=n).astype("timedelta64[D]")

    cb_rate = np.empty(n, dtype=np.float64)
    for rl, (lo, hi) in [("HIGH", (0.05, 0.25)), ("MEDIUM", (0.01, 0.05)), ("LOW", (0.001, 0.01))]:
        mask = risk == rl
        cb_rate[mask] = np.random.uniform(lo, hi, size=mask.sum())
    cb_rate = np.round(cb_rate, 6)
    null_cb = np.random.random(n) < 0.01
    cb_series = pd.array(cb_rate, dtype=pd.Float64Dtype())
    cb_series[null_cb] = pd.NA

    is_fatf = np.isin(countries, list(FATF_HIGH_RISK)).astype(np.int8)

    cities = np.array([
        np.random.choice(COUNTRY_CITIES.get(c, ["Unknown"])) for c in countries
    ], dtype=object)
    cities[np.random.random(n) < 0.02] = None

    vol = np.empty(n, dtype=np.int32)
    for rl, (lo, hi) in [("LOW", (1000, 50000)), ("MEDIUM", (500, 20000)), ("HIGH", (100, 5000))]:
        mask = risk == rl
        vol[mask] = np.random.randint(lo, hi, size=mask.sum())

    df = pd.DataFrame({
        "merchant_id": merch_ids,
        "merchant_category": pd.Categorical(categories),
        "merchant_country": pd.Categorical(countries),
        "merchant_risk_level": pd.Categorical(risk, categories=MERCHANT_RISK_LEVELS),
        "mcc_code": mcc,
        "merchant_since": merch_since,
        "chargeback_rate": cb_series,
        "is_high_risk_jurisdiction": is_fatf,
        "merchant_city": cities,
        "annual_txn_volume": vol,
    })

    save_csv(df, "merchants.csv")
    print_validation(df, "merchants.csv",
                     cat_cols=["merchant_risk_level", "merchant_category", "is_high_risk_jurisdiction"],
                     num_cols=["chargeback_rate", "annual_txn_volume"])
    return df


# ============================================================
# 3. DEVICES (100K) — ALL FIELDS
# ============================================================
@timer
def generate_devices():
    print("\n" + "=" * 70)
    print("GENERATING: devices.csv (ALL FIELDS)")
    print("=" * 70)
    n = NUM_DEVICES
    dev_ids = np.arange(1, n + 1)

    dev_types = np.random.choice(DEVICE_TYPES, size=n, p=[0.55, 0.30, 0.15])

    octets = np.random.randint(1, 255, size=(n, 4))
    ip_addresses = np.array([f"{o[0]}.{o[1]}.{o[2]}.{o[3]}" for o in octets])

    countries = np.random.choice(COUNTRIES_CLEAN, size=n, p=COUNTRY_WEIGHTS)
    gb_mask = countries == "GB"
    if gb_mask.sum() > 0:
        countries[gb_mask] = np.random.choice(GB_VARIANTS, size=gb_mask.sum())

    is_vpn = (np.random.random(n) < 0.05).astype(np.int8)

    base = np.datetime64("2025-12-31")
    first_seen = base - np.random.randint(0, 1461, size=n).astype("timedelta64[D]")

    device_os = np.empty(n, dtype=object)
    mobile_mask = dev_types == "Mobile"
    desktop_mask = dev_types == "Desktop"
    tablet_mask = dev_types == "Tablet"

    device_os[mobile_mask] = np.random.choice(["iOS", "Android"], size=mobile_mask.sum(), p=[0.60, 0.40])
    device_os[desktop_mask] = np.random.choice(["Windows", "macOS", "Linux"], size=desktop_mask.sum(), p=[0.55, 0.30, 0.15])
    device_os[tablet_mask] = np.random.choice(["iPadOS", "Android"], size=tablet_mask.sum(), p=[0.55, 0.45])

    accts_on_dev = np.random.choice(
        [1, 2, 3, 4, 5, 6, 7, 8], size=n,
        p=[0.85, 0.10, 0.04, 0.004, 0.002, 0.002, 0.001, 0.001]
    )

    charset = np.array(list(string.ascii_lowercase + string.digits))
    fp_chars = np.random.choice(charset, size=(n, 16))
    fingerprints = np.array(["".join(row) for row in fp_chars], dtype=object)
    null_fp = (np.random.random(n) < 0.10) | (mobile_mask & (np.random.random(n) < 0.20))
    fingerprints[null_fp] = None

    df = pd.DataFrame({
        "device_id": dev_ids,
        "device_type": pd.Categorical(dev_types),
        "ip_address": ip_addresses,
        "device_country": countries,
        "is_vpn": is_vpn,
        "device_first_seen": first_seen,
        "device_os": pd.Categorical(device_os),
        "accounts_on_device": accts_on_dev,
        "browser_fingerprint": fingerprints,
    })

    save_csv(df, "devices.csv")
    print_validation(df, "devices.csv",
                     cat_cols=["device_type", "device_os", "is_vpn", "accounts_on_device"],
                     num_cols=[])
    return df


# ============================================================
# 4. FX RATES
# ============================================================
@timer
def generate_fx_rates():
    print("\n" + "=" * 70)
    print("GENERATING: fx_rates.csv")
    print("=" * 70)
    dates = pd.date_range(start=START_DATE, end=END_DATE, freq="D")
    n_days = len(dates)
    base_rates = {"USD": 1.0, "GBP": 0.79, "INR": 83.5, "EUR": 0.92}

    records = []
    for currency, base in base_rates.items():
        daily_changes = np.random.normal(0, 0.002, size=n_days)
        cumulative = np.cumsum(daily_changes)
        rates = base * (1 + cumulative)
        rates = np.clip(rates, base * 0.8, base * 1.2)

        records.append(pd.DataFrame({
            "rate_date": dates,
            "currency": currency,
            "usd_rate": np.round(rates, 6),
        }))

    df = pd.concat(records, ignore_index=True)
    df["currency"] = pd.Categorical(df["currency"])

    save_csv(df, "fx_rates.csv")
    print_validation(df, "fx_rates.csv", cat_cols=["currency"], num_cols=["usd_rate"])
    return df


# ============================================================
# 5. TRANSACTIONS (1M) — ALL FIELDS
# ============================================================
@timer
def generate_transactions(customers_df, merchants_df, devices_df):
    print("\n" + "=" * 70)
    print("GENERATING: transactions.csv (ALL FIELDS)")
    print("=" * 70)
    n = NUM_TRANSACTIONS

    cust_ids = customers_df["customer_id"].values
    income_bands = customers_df["income_band"].values.astype(str)
    risk_profiles = customers_df["risk_profile"].values.astype(str)
    home_countries = customers_df["home_country"].values.astype(str)
    n_cust = len(cust_ids)

    print("  Assigning customer behavior classes...")
    cust_behavior = np.empty(n_cust, dtype=object)
    for rp, probs in [("HIGH", [0.75, 0.125, 0.125]),
                       ("MEDIUM", [0.88, 0.07, 0.05]),
                       ("LOW", [0.95, 0.035, 0.015])]:
        mask = risk_profiles == rp
        cust_behavior[mask] = np.random.choice(
            ["normal", "medium_anomaly", "extreme_anomaly"], size=mask.sum(), p=probs
        )

    income_lookup = np.empty(n_cust + 1, dtype=object)
    income_lookup[cust_ids] = income_bands
    home_lookup = np.empty(n_cust + 1, dtype=object)
    home_lookup[cust_ids] = home_countries
    behavior_lookup = np.empty(n_cust + 1, dtype=object)
    behavior_lookup[cust_ids] = cust_behavior

    print("  Assigning customers to transactions...")
    txn_cust_ids = np.random.choice(cust_ids, size=n, replace=True)

    accts_per = np.random.randint(10, 41, size=n_cust)
    acct_lookup = np.zeros(n_cust + 1, dtype=np.int32)
    acct_lookup[cust_ids] = accts_per
    txn_acct_offset = np.minimum(np.random.randint(0, 40, size=n), acct_lookup[txn_cust_ids] - 1)
    txn_acct_ids = txn_cust_ids * 100 + txn_acct_offset

    txn_ids = np.arange(1, n + 1)
    n_dupes = int(n * DUPLICATE_TXN_FRAC)
    txn_ids[np.random.choice(n, n_dupes, replace=False)] = \
        txn_ids[np.random.choice(n, n_dupes, replace=False)]

    print("  Generating timestamps...")
    random_secs = np.random.randint(0, DATE_RANGE_SECONDS, size=n)
    base_ts = np.datetime64(START_DATE)
    txn_timestamps = base_ts + random_secs.astype("timedelta64[s]")

    txn_behaviors = behavior_lookup[txn_cust_ids]
    txn_incomes = income_lookup[txn_cust_ids]
    txn_homes = home_lookup[txn_cust_ids]

    normal_mask = txn_behaviors == "normal"
    medium_mask = txn_behaviors == "medium_anomaly"
    extreme_mask = txn_behaviors == "extreme_anomaly"

    print("  Generating amounts...")
    amounts = np.empty(n, dtype=np.float64)
    for band in INCOME_BANDS:
        mask = txn_incomes == band
        amounts[mask] = np.random.lognormal(INCOME_SPEND_MU[band], INCOME_SPEND_SIGMA[band], size=mask.sum())

    spike_med = medium_mask & (np.random.random(n) < 0.3)
    amounts[spike_med] *= np.random.uniform(2, 5, size=spike_med.sum())

    spike_ext = extreme_mask & (np.random.random(n) < 0.5)
    amounts[spike_ext] *= np.random.uniform(5, 10, size=spike_ext.sum())

    amounts = np.round(amounts, 2)

    neg_idx = np.random.choice(n, int(n * NEGATIVE_AMOUNT_FRAC), replace=False)
    amounts[neg_idx] = -np.abs(amounts[neg_idx])

    zero_idx = np.random.choice(n, int(n * ZERO_AMOUNT_FRAC), replace=False)
    amounts[zero_idx] = 0.0

    currencies = np.random.choice(CURRENCIES, size=n, p=[0.40, 0.15, 0.25, 0.20])
    mixed_n = int(n * 0.001)
    currencies[np.random.choice(n, mixed_n, replace=False)] = np.random.choice(
        ["usd", "Usd", "gbp", "Gbp", "inr", "Inr", "eur", "Eur"], size=mixed_n
    )

    channels = np.random.choice(CHANNELS, size=n, p=[0.30, 0.35, 0.15, 0.20])
    txn_types = np.random.choice(TXN_TYPES, size=n, p=[0.65, 0.35])

    print("  Assigning merchants...")
    merch_ids = merchants_df["merchant_id"].values
    merch_risks = merchants_df["merchant_risk_level"].values.astype(str)
    merch_cats = merchants_df["merchant_category"].values.astype(str)
    high_risk_merch = merch_ids[merch_risks == "HIGH"]

    txn_merch_ids = np.random.choice(merch_ids, size=n, replace=True)
    if len(high_risk_merch) > 0:
        hr_switch = (medium_mask | extreme_mask) & (np.random.random(n) < 0.40)
        txn_merch_ids[hr_switch] = np.random.choice(high_risk_merch, size=hr_switch.sum(), replace=True)

    print("  Assigning devices...")
    dev_ids = devices_df["device_id"].values
    txn_dev_ids = np.random.choice(dev_ids, size=n, replace=True).astype(object)

    new_dev = extreme_mask & (np.random.random(n) < 0.3)
    txn_dev_ids[new_dev] = np.random.choice(dev_ids, size=new_dev.sum(), replace=True)

    null_dev = np.random.choice(n, int(n * NULL_DEVICE_FRAC), replace=False)
    txn_dev_ids[null_dev] = None

    print("  Generating transaction countries...")
    txn_countries = txn_homes.copy()

    for mask, prob in [(normal_mask, 0.05), (medium_mask, 0.20), (extreme_mask, 0.50)]:
        foreign = mask & (np.random.random(n) < prob)
        txn_countries[foreign] = np.random.choice(COUNTRIES_CLEAN, size=foreign.sum(), p=COUNTRY_WEIGHTS)

    gb_mask = txn_countries == "GB"
    if gb_mask.sum() > 0:
        dirty_n = int(gb_mask.sum() * 0.3)
        dirty_idx = np.where(gb_mask)[0][:dirty_n]
        txn_countries[dirty_idx] = np.random.choice(GB_VARIANTS, size=len(dirty_idx))

    print("  Injecting burst patterns...")
    extreme_idx = np.where(extreme_mask)[0]
    if len(extreme_idx) > 100:
        burst_custs = np.random.choice(
            np.unique(txn_cust_ids[extreme_idx]),
            size=min(500, len(np.unique(txn_cust_ids[extreme_idx]))),
            replace=False
        )
        for bc in burst_custs:
            bc_idx = np.where((txn_cust_ids == bc) & extreme_mask)[0]
            if len(bc_idx) >= 5:
                burst = bc_idx[:np.random.randint(5, min(11, len(bc_idx) + 1))]
                offsets = np.random.randint(0, 300, size=len(burst))
                txn_timestamps[burst] = txn_timestamps[burst[0]] + offsets.astype("timedelta64[s]")

    print("  Corrupting timestamps...")
    ts_strings = txn_timestamps.astype("datetime64[s]").astype(str)
    bad_n = int(n * BAD_TIMESTAMP_FRAC)
    bad_idx = np.random.choice(n, bad_n, replace=False)
    corruptions = np.random.choice(["slash", "dot", "extra_space", "missing_T"], size=bad_n)
    for i, idx in enumerate(bad_idx):
        s = ts_strings[idx]
        if corruptions[i] == "slash":
            ts_strings[idx] = s[:10].replace("-", "/") + s[10:]
        elif corruptions[i] == "dot":
            ts_strings[idx] = s[:10].replace("-", ".") + s[10:]
        elif corruptions[i] == "extra_space":
            ts_strings[idx] = s + "  "
        elif corruptions[i] == "missing_T":
            ts_strings[idx] = s.replace("T", " ") if "T" in s else s

    # ---- EXTENDED COLUMNS ----
    print("  Generating extended transaction fields...")

    ts_clean = txn_timestamps.astype("datetime64[s]")
    ts_series = pd.Series(ts_clean)
    txn_date = ts_series.dt.strftime("%Y-%m-%d").values
    txn_hour = ts_series.dt.hour.values.astype(np.int32)
    txn_dow = ts_series.dt.dayofweek.values.astype(np.int32)

    rand_bill = np.random.random(n)
    billing_country = np.where(
        rand_bill < 0.85, txn_countries,
        np.where(rand_bill < 0.95, txn_homes,
                 np.random.choice(COUNTRIES_CLEAN, size=n))
    )

    txn_status = np.random.choice(
        ["COMPLETED", "DECLINED", "REVERSED", "PENDING"],
        size=n, p=[0.92, 0.04, 0.02, 0.02]
    )

    response_code = np.empty(n, dtype=object)
    response_code[txn_status == "COMPLETED"] = "00"
    response_code[txn_status == "REVERSED"] = "09"
    response_code[txn_status == "PENDING"] = "68"
    declined = txn_status == "DECLINED"
    response_code[declined] = np.random.choice(["05", "14", "51", "57"], size=declined.sum())

    entry_mode = np.empty(n, dtype=object)
    pos_m = channels == "POS"
    entry_mode[pos_m] = np.random.choice(
        ["CHIP", "CONTACTLESS", "SWIPE", "MANUAL"], size=pos_m.sum(), p=[0.50, 0.30, 0.15, 0.05]
    )
    entry_mode[channels == "ONLINE"] = "CNP"
    entry_mode[channels == "ATM"] = "CHIP"
    entry_mode[channels == "MOBILE"] = "CNP"

    is_recurring = np.zeros(n, dtype=np.int8)
    online_m = channels == "ONLINE"
    mobile_m = channels == "MOBILE"
    is_recurring[online_m] = (np.random.random(online_m.sum()) < 0.25).astype(np.int8)
    is_recurring[mobile_m] = (np.random.random(mobile_m.sum()) < 0.15).astype(np.int8)

    gb_set = {"UK", "United Kingdom", "Britain", "Great Britain", "U.K.", "GB"}
    txn_norm = np.where(np.isin(txn_countries, list(gb_set)), "GB", txn_countries)
    home_norm = np.where(np.isin(txn_homes, list(gb_set)), "GB", txn_homes)
    is_international = (txn_norm != home_norm).astype(np.int8)

    merch_cat_lookup = dict(zip(merchants_df["merchant_id"].values, merch_cats))
    txn_merch_cats = np.array([merch_cat_lookup.get(m, "Other") for m in txn_merch_ids])
    mcc_code = np.array([MCC_MAP.get(cat, MCC_DEFAULT) for cat in txn_merch_cats], dtype=np.int32)

    dispute = np.zeros(n, dtype=np.int8)
    rev_m = txn_status == "REVERSED"
    comp_m = txn_status == "COMPLETED"
    dispute[rev_m] = (np.random.random(rev_m.sum()) < 0.60).astype(np.int8)
    dispute[comp_m] = (np.random.random(comp_m.sum()) < 0.005).astype(np.int8)

    print("  Assembling transactions DataFrame...")
    df = pd.DataFrame({
        "txn_id": txn_ids,
        "account_id": txn_acct_ids,
        "customer_id": txn_cust_ids,
        "txn_timestamp": ts_strings,
        "txn_date": txn_date,
        "txn_hour": txn_hour,
        "txn_day_of_week": txn_dow,
        "amount": amounts,
        "currency": currencies,
        "merchant_id": txn_merch_ids,
        "channel": pd.Categorical(channels),
        "txn_country": txn_countries,
        "billing_country": billing_country,
        "device_id": txn_dev_ids,
        "txn_type": pd.Categorical(txn_types, categories=TXN_TYPES),
        "txn_status": pd.Categorical(txn_status),
        "response_code": response_code,
        "entry_mode": pd.Categorical(entry_mode),
        "is_recurring": is_recurring,
        "is_international": is_international,
        "mcc_code": mcc_code,
        "dispute_flag": dispute,
    })

    save_csv(df, "transactions.csv")
    print_validation(df, "transactions.csv",
                     cat_cols=["txn_status", "entry_mode", "channel", "is_recurring",
                               "is_international", "dispute_flag"],
                     num_cols=["amount", "txn_hour", "mcc_code"])
    return df, txn_behaviors


# ============================================================
# 6. ACCOUNTS (NEW TABLE) — BUG FIXED
# ============================================================
@timer
def generate_accounts(customers_df):
    print("\n" + "=" * 70)
    print("GENERATING: accounts.csv (NEW TABLE)")
    print("=" * 70)

    cust_ids = customers_df["customer_id"].values
    num_accts = customers_df["num_accounts"].values
    segments = customers_df["customer_segment"].values.astype(str)
    risks = customers_df["risk_profile"].values.astype(str)
    credit_limits = customers_df["credit_limit"].values
    balances = customers_df["current_balance"].values
    home_countries = customers_df["home_country"].values.astype(str)
    pref_currencies = customers_df["preferred_currency"].values.astype(str)
    cust_since = pd.to_datetime(customers_df["customer_since"]).values

    print("  Expanding accounts per customer...")
    repeat_idx = np.repeat(np.arange(len(cust_ids)), num_accts)
    n = len(repeat_idx)
    print(f"  Total accounts: {n:,}")

    a_cust = cust_ids[repeat_idx]
    a_seg = segments[repeat_idx]
    a_risk = risks[repeat_idx]
    a_cl = credit_limits[repeat_idx]
    a_bal = balances[repeat_idx]
    a_home = home_countries[repeat_idx]
    a_curr = pref_currencies[repeat_idx]
    a_since = cust_since[repeat_idx]

    # account_id
    print("  Generating account_id...")
    acct_ids = np.array([f"ACC_{i:08d}" for i in range(1, n + 1)])

    # account_type
    print("  Generating account_type...")
    acct_type = np.empty(n, dtype=object)
    for seg, types, probs in [
        ("Retail", ["CURRENT", "SAVINGS", "CREDIT"], [0.70, 0.20, 0.10]),
        ("SME", ["CURRENT", "BUSINESS", "CREDIT"], [0.50, 0.30, 0.20]),
        ("Corporate", ["CORPORATE", "CURRENT", "CREDIT"], [0.40, 0.35, 0.25])
    ]:
        mask = a_seg == seg
        acct_type[mask] = np.random.choice(types, size=mask.sum(), p=probs)

    # account_status
    print("  Generating account_status...")
    acct_status = np.random.choice(
        ["ACTIVE", "FROZEN", "UNDER_REVIEW", "CLOSED"],
        size=n, p=[0.88, 0.06, 0.04, 0.02]
    )
    high_risk = (a_risk == "HIGH") & (np.random.random(n) < 0.20)
    acct_status[high_risk] = np.random.choice(["FROZEN", "UNDER_REVIEW"], size=high_risk.sum())

    # account_open_date
    print("  Generating account_open_date...")
    seq_within = np.concatenate([np.arange(na) for na in num_accts])

    # Start with customer_since for all accounts
    acct_open = a_since.copy()

    # Additional accounts (seq > 0): random date AFTER customer_since
    additional = seq_within > 0
    if additional.sum() > 0:
        end = np.datetime64("2025-12-31")
        max_days = ((end - a_since[additional]) / np.timedelta64(1, "D")).astype(np.int64)
        max_days = np.maximum(max_days, 1)
        offset = (np.random.random(additional.sum()) * max_days).astype(np.int64)
        acct_open[additional] = a_since[additional] + offset.astype("timedelta64[D]")

    # ============================================================
    # FIX: Convert numpy datetime64 array to string directly
    #       pd.to_datetime() returns DatetimeIndex which has no .dt
    #       Instead: use pd.Series() wrapper for .dt accessor
    # ============================================================
    acct_open_strings = pd.Series(pd.to_datetime(acct_open)).dt.strftime("%Y-%m-%d").values

    print("  Assembling accounts DataFrame...")
    df = pd.DataFrame({
        "account_id": acct_ids,
        "customer_id": a_cust,
        "account_type": pd.Categorical(acct_type),
        "account_status": pd.Categorical(acct_status),
        "credit_limit": np.round(a_cl, 2),
        "current_balance": np.round(a_bal, 2),
        "account_open_date": acct_open_strings,
        "account_country": a_home,
        "currency": a_curr,
    })

    save_csv(df, "accounts.csv")
    print_validation(df, "accounts.csv",
                     cat_cols=["account_type", "account_status", "currency"],
                     num_cols=["credit_limit", "current_balance"])
    return df


# ============================================================
# FINAL VALIDATION
# ============================================================
def final_validation(txn_df, cust_df, merch_df, dev_df, fx_df, acct_df, txn_behaviors):
    print("\n" + "=" * 70)
    print("FINAL VALIDATION")
    print("=" * 70)

    unique, counts = np.unique(txn_behaviors, return_counts=True)
    print(f"\nTransaction behavior distribution:")
    for beh, cnt in zip(unique, counts):
        print(f"  {beh:<20} {cnt:>10,} ({cnt / len(txn_behaviors) * 100:.2f}%)")

    extreme_pct = (txn_behaviors == "extreme_anomaly").sum() / len(txn_behaviors) * 100
    medium_pct = (txn_behaviors == "medium_anomaly").sum() / len(txn_behaviors) * 100
    print(f"\n% extreme anomaly: {extreme_pct:.2f}%")
    print(f"% medium anomaly:  {medium_pct:.2f}%")

    amounts = pd.to_numeric(txn_df["amount"], errors="coerce")
    print(f"\nAmount percentiles:")
    for p in [1, 5, 10, 25, 50, 75, 90, 95, 99]:
        print(f"  P{p:>2}: {amounts.quantile(p / 100):>12.2f}")

    print(f"\nNegative amounts: {(amounts < 0).sum():,} ({(amounts < 0).mean() * 100:.3f}%)")
    print(f"Zero amounts:     {(amounts == 0).sum():,} ({(amounts == 0).mean() * 100:.3f}%)")
    print(f"Duplicate txn_ids: {txn_df['txn_id'].duplicated().sum():,}")
    print(f"Null device_ids:   {txn_df['device_id'].isna().sum():,}")

    tpc = txn_df["customer_id"].value_counts()
    print(f"\nTransactions per customer:")
    print(f"  Min: {tpc.min()} | Max: {tpc.max()} | Mean: {tpc.mean():.1f} | Median: {tpc.median():.0f}")
    for p in [5, 25, 50, 75, 95]:
        print(f"  P{p:>2}: {tpc.quantile(p / 100):.0f}")

    print(f"\nTop 10 transaction countries:")
    for c, cnt in txn_df["txn_country"].value_counts().head(10).items():
        print(f"  {c:<20} {cnt:>10,} ({cnt / len(txn_df) * 100:.2f}%)")

    merch_risk_lookup = dict(zip(merch_df["merchant_id"], merch_df["merchant_risk_level"]))
    txn_merch_risk = txn_df["merchant_id"].map(merch_risk_lookup)
    print(f"\nMerchant risk level in transactions:")
    for rl, cnt in txn_merch_risk.value_counts().items():
        print(f"  {rl:<10} {cnt:>10,} ({cnt / len(txn_df) * 100:.2f}%)")

    print(f"\nAccounts per customer:")
    apc = acct_df["customer_id"].value_counts()
    print(f"  Min: {apc.min()} | Max: {apc.max()} | Mean: {apc.mean():.2f}")

    print(f"\nFinal file sizes:")
    total_size = 0
    for fname in ["transactions.csv", "customers.csv", "devices.csv",
                   "merchants.csv", "accounts.csv", "fx_rates.csv"]:
        fpath = os.path.join(OUTPUT_DIR, fname)
        if os.path.exists(fpath):
            size = os.path.getsize(fpath) / (1024 * 1024)
            total_size += size
            print(f"  {fname:<25} {size:>8.1f} MB")
    print(f"  {'TOTAL':<25} {total_size:>8.1f} MB")

    print(f"\n✅ No fraud_flag column in any file.")
    print(f"✅ Fraud to be derived from behavioral signals in SQL layer.")


# ============================================================
# MAIN
# ============================================================
def main():
    print("=" * 70)
    print("COMPLETE BANKING DATASET GENERATOR (ALL FIELDS)")
    print(f"Transactions: {NUM_TRANSACTIONS:,} | Customers: {NUM_CUSTOMERS:,}")
    print(f"Merchants: {NUM_MERCHANTS:,} | Devices: {NUM_DEVICES:,}")
    print(f"Output: {OUTPUT_DIR}")
    print("=" * 70)

    t_start = time.time()

    customers_df = generate_customers()
    merchants_df = generate_merchants()
    devices_df = generate_devices()
    fx_df = generate_fx_rates()
    transactions_df, txn_behaviors = generate_transactions(customers_df, merchants_df, devices_df)
    accounts_df = generate_accounts(customers_df)

    total_time = time.time() - t_start
    print(f"\n⏱️  Total generation time: {total_time:.2f}s")

    final_validation(transactions_df, customers_df, merchants_df,
                     devices_df, fx_df, accounts_df, txn_behaviors)

    print("\n" + "=" * 70)
    print(f"🎉 DATASET GENERATION COMPLETE")
    print(f"📂 Files saved to: {OUTPUT_DIR}")
    print("=" * 70)


if __name__ == "__main__":
    main()