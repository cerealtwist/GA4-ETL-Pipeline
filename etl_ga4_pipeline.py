"""
=============================================================================
GA4 E-Commerce Marketing Performance ETL Pipeline
For: Google Colab, local terminal, AND GitHub Actions scheduler
=============================================================================
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime

#CONFIG
#Reads SPREADSHEET_NAME from environment var (set in GitHub Actions secret)
#Falls back to a hardcoded value if running locally
RAW_FILE         = "ga4_obfuscated_sample_ecommerce_Jan2021_-_ga4_event_2021.csv"
SPREADSHEET_NAME = os.environ.get("SPREADSHEET_NAME", "GA4 Marketing Dashboard")
SERVICE_ACCT_KEY = "service_account.json"
OUTPUT_DIR       = "outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

RUN_TS = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(f"[{RUN_TS}] GA4 ETL Pipeline starting...")
print(f"          Spreadsheet: {SPREADSHEET_NAME}")

# ============================================================================
# STEP 1 — EXTRACT
# ============================================================================
print("\n[1/4] Extracting raw data")
raw_df = pd.read_csv(RAW_FILE)
print(f"      Raw rows: {len(raw_df):,}  |  Columns: {len(raw_df.columns)}")

# ============================================================================
# STEP 2 — TRANSFORM
# ============================================================================
print("\n[2/4] Transforming data")

META_COLS = [
    "event_date", "event_timestamp", "event_name",
    "user_pseudo_id", "user_ltv.revenue", "user_ltv.currency",
    "device.category", "device.operating_system", "device.web_info.browser",
    "geo.continent", "geo.country", "geo.region", "geo.city",
    "traffic_source.source", "traffic_source.medium", "traffic_source.name",
    "ecommerce.purchase_revenue_in_usd", "ecommerce.transaction_id",
    "ecommerce.total_item_quantity", "ecommerce.unique_items",
    "ecommerce.shipping_value_in_usd", "ecommerce.tax_value_in_usd",
]

df = raw_df.copy()
df[META_COLS] = df[META_COLS].ffill()

events_df = (
    df
    .drop_duplicates(subset=["event_timestamp", "user_pseudo_id", "event_name"])
    .query("event_date == event_date")
    .copy()
)

# Parse & clean
events_df["event_date"] = pd.to_datetime(
    events_df["event_date"].astype(int).astype(str), format="%Y%m%d"
)
events_df["week"]  = events_df["event_date"].dt.to_period("W").astype(str)
events_df["month"] = events_df["event_date"].dt.to_period("M").astype(str)

PLACEHOLDER_MAP = {"(data deleted)": "unknown", "<Other>": "other", "(not set)": "unknown"}
for col in ["traffic_source.source", "traffic_source.medium", "traffic_source.name"]:
    events_df[col] = events_df[col].replace(PLACEHOLDER_MAP)

events_df["is_session"]     = events_df["event_name"].isin({"session_start", "first_visit"})
events_df["is_new_user"]    = events_df["event_name"] == "first_visit"
events_df["is_engaged"]     = events_df["event_name"].isin(
    {"scroll", "user_engagement", "view_item", "select_promotion", "view_promotion"}
)
events_df["revenue"]        = pd.to_numeric(
    events_df["ecommerce.purchase_revenue_in_usd"], errors="coerce"
).fillna(0)
events_df["is_transaction"] = (
    events_df["ecommerce.transaction_id"].notna()
    & (events_df["ecommerce.transaction_id"] != "(not set)")
)

# Aggregate KPI tables
daily = (
    events_df.groupby("event_date")
    .agg(sessions=("is_session","sum"), new_users=("is_new_user","sum"),
         total_users=("user_pseudo_id","nunique"), events=("event_name","count"),
         engaged_events=("is_engaged","sum"), revenue=("revenue","sum"),
         transactions=("is_transaction","sum"))
    .reset_index()
)
daily["engagement_rate"] = (daily["engaged_events"] / daily["events"]).round(3)
daily["aov"] = np.where(daily["transactions"]>0, daily["revenue"]/daily["transactions"], 0)
daily["event_date"] = daily["event_date"].dt.strftime("%Y-%m-%d")

traffic = (
    events_df.groupby(["traffic_source.source","traffic_source.medium"])
    .agg(sessions=("is_session","sum"), users=("user_pseudo_id","nunique"),
         events=("event_name","count"), revenue=("revenue","sum"))
    .reset_index()
    .rename(columns={"traffic_source.source":"source","traffic_source.medium":"medium"})
    .sort_values("sessions", ascending=False)
)

device = (
    events_df.groupby("device.category")
    .agg(sessions=("is_session","sum"), users=("user_pseudo_id","nunique"),
         events=("event_name","count"))
    .reset_index().rename(columns={"device.category":"device_category"})
)

geo = (
    events_df.groupby(["geo.country","geo.city"])
    .agg(sessions=("is_session","sum"), users=("user_pseudo_id","nunique"),
         events=("event_name","count"))
    .reset_index()
    .rename(columns={"geo.country":"country","geo.city":"city"})
    .sort_values("sessions", ascending=False)
)

funnel = (
    events_df.groupby("event_name")
    .agg(count=("event_name","count"), users=("user_pseudo_id","nunique"))
    .reset_index().sort_values("count", ascending=False)
)

items_raw = raw_df[
    raw_df["items.item_name"].notna() & (raw_df["items.item_name"] != "(not set)")
].copy()
top_items = (
    items_raw.groupby(["items.item_name","items.item_category"])
    .agg(views=("items.item_name","count"), avg_price=("items.price_in_usd","mean"))
    .reset_index()
    .rename(columns={"items.item_name":"item_name","items.item_category":"item_category"})
    .sort_values("views", ascending=False)
)

outputs = {
    "daily_summary":  daily,
    "traffic_source": traffic,
    "device":         device,
    "geography":      geo,
    "event_funnel":   funnel,
    "top_items":      top_items,
}

print(f"      Clean events: {len(events_df):,}  |  Date range: "
      f"{events_df['event_date'].min().date()} to {events_df['event_date'].max().date()}")

# ============================================================================
# STEP 3 — LOAD
# ============================================================================
print("\n[3/4] Loading outputs...")

# Always write CSVs
for name, df_out in outputs.items():
    path = os.path.join(OUTPUT_DIR, f"{name}.csv")
    df_out.to_csv(path, index=False)
    print(f"CSV: {path}")

# Push to Google Sheets (only if key file exists)
if os.path.exists(SERVICE_ACCT_KEY):
    try:
        import gspread
        from gspread_dataframe import set_with_dataframe
        from google.oauth2.service_account import Credentials

        SCOPES = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = Credentials.from_service_account_file(SERVICE_ACCT_KEY, scopes=SCOPES)
        gc    = gspread.authorize(creds)
        sh    = gc.open(SPREADSHEET_NAME)

        for sheet_name, df_out in outputs.items():
            try:
                ws = sh.worksheet(sheet_name)
                ws.clear()
            except gspread.WorksheetNotFound:
                ws = sh.add_worksheet(title=sheet_name, rows=500, cols=30)
            set_with_dataframe(ws, df_out)
            print(f"Google Sheets tab updated: {sheet_name}")

        print("\n      All tabs updated in Google Sheets!")

    except Exception as e:
        print(f"\n      ERROR pushing to Google Sheets: {e}")
        raise   #re-raise so GitHub Actions marks the run as failed
else:
    print(f"\n      Skipping Google Sheets push - '{SERVICE_ACCT_KEY}' not found.")
    print("      (This is normal if running without a service account key.)")

# ============================================================================
# STEP 4 — SUMMARY
# ============================================================================
print("\n[4/4] Pipeline complete.")
print("=" * 55)
print(f"  Sessions        : {int(events_df['is_session'].sum())}")
print(f"  Unique Users    : {events_df['user_pseudo_id'].nunique()}")
print(f"  New Users       : {int(events_df['is_new_user'].sum())}")
print(f"  Engagement Rate : {events_df['is_engaged'].mean()*100:.1f}%")
print(f"  Revenue         : ${events_df['revenue'].sum():.2f}")
print(f"  Last Refreshed  : {RUN_TS}")
print("=" * 55)
