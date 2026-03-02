# GA4 Marketing Performance Dashboard — ETL Pipeline

An automated end-to-end ETL pipeline for Google Analytics 4 e-commerce data. Extracts and transforms raw GA4 event data, loads 14 KPI tables into Google Sheets, and powers a live Looker Studio dashboard — refreshed automatically every day via GitHub Actions.

> Built as part of the **Comms8 Data Intern – Data & Automation Assessment**  
> **Author:** Farand Mahazalfaa

---

## 🔗 Live Deliverables

| Deliverable | Link |
|---|---|
| Looker Studio Dashboard | [View Dashboard](https://lookerstudio.google.com/reporting/4c74e190-3219-4f19-9350-951b57795f14) |
| Google Colab Notebook | `GA4_Marketing_Dashboard_ETL_v2.ipynb` (this repo) |

---

## Architecture

```
GA4 Raw CSV Dataset
        ↓
Python ETL Script  ←  GitHub Actions (runs daily 06:00 UTC)
        ↓
14 KPI Tables (Google Sheets)
        ↓
Looker Studio Dashboard  ←  auto-refreshes on data change
```

---

## Project Structure

```
GA4-ETL-Pipeline/
├── .github/
│   └── workflows/
│       └── refresh.yml              ← GitHub Actions scheduler
├── outputs/                         ← Generated CSVs (auto-updated per run)
├── etl_ga4_pipeline.py              ← Main ETL script
├── GA4_Marketing_Dashboard_ETL_v2.ipynb  ← Full Colab notebook
├── ga4_obfuscated_sample_ecommerce_Jan2021_-_ga4_event_2021.csv
├── requirements.txt
└── README.md
```

---

## ETL Pipeline (What it Does?)

### Extract
Reads the GA4 BigQuery flat-export CSV. GA4 exports use a nested schema where each event has one header row plus several parameter sub-rows. The script forward-fills event metadata and deduplicates to reconstruct 52 clean event-level records from 500 raw rows.

### Transform
Derives behavioural flags and aggregates **14 KPI tables**:

| # | Table | Contents |
|---|---|---|
| 1 | `daily_summary` | Sessions, users, engagement rate per day |
| 2 | `traffic_source` | Raw channel breakdown |
| 3 | `traffic_quality` | Channel volume vs engagement rate (quality) |
| 4 | `device` | Desktop vs mobile |
| 5 | `geography` | Country & city breakdown |
| 6 | `event_funnel` | All event types and counts |
| 7 | `top_items` | Most viewed products |
| 8 | `new_vs_returning` | New vs returning user split |
| 9 | `promotion_funnel` | Viewed promo -> clicked promo funnel |
| 10 | `browser_breakdown` | Browser share |
| 11 | `os_breakdown` | Operating system share |
| 12 | `day_of_week` | Activity by day of week |
| 13 | `session_depth` | Events per user distribution |
| 14 | `scroll_rate` | Content scroll engagement rate |

### Load
Pushes all 14 tables to Google Sheets via `gspread` authenticated with a Google Cloud service account. Each tab is cleared and rewritten on every run — simulating a live data refresh. Looker Studio reads directly from these tabs.

---

## GitHub Actions

The pipeline runs automatically every day at **06:00 UTC** via a cron-scheduled GitHub Actions workflow. A manual trigger is also available via the Actions tab.

```yaml
on:
  schedule:
    - cron: '0 6 * * *'   # daily at 06:00 UTC
  workflow_dispatch:        # manual trigger
```

**Each run:**
1. Spins up a free GitHub-hosted Ubuntu runner
2. Installs Python dependencies from `requirements.txt`
3. Writes the service account key from encrypted GitHub Secrets
4. Executes the full ETL pipeline
5. Saves output CSVs as downloadable run artifacts
6. Commits refreshed CSVs back to the repository

---

### Prerequisites
- Python 3.11+
- Google Cloud project with Sheets API + Drive API enabled
- Service account with Editor access to the target Google Sheet
- GitHub repository with two Secrets configured

### GitHub Secrets Required

| Secret | Value |
|---|---|
| `SERVICE_ACCOUNT_JSON` | Full contents of your `service_account.json` file |
| `SPREADSHEET_NAME` | Exact name of your Google Sheet |


## Dashboard (10 Visualizations)

| # | Chart | Type | Insight |
|---|---|---|---|
| 1 | Most Viewed Products | Treemap | Product discovery interest |
| 2 | Traffic Channel Quality | Combo (Bar + Line) | Volume vs engagement quality per channel |
| 3 | New vs Returning Users | Donut + Scorecards | Acquisition vs retention balance |
| 4 | Geographic Breakdown | Geo Map + Table | Country and city-level traffic |
| 5 | Promotion Funnel | Funnel Bar | Viewed → clicked CTR |
| 6 | Browser Distribution | Donut | Browser share for QA prioritisation |
| 7 | OS Distribution | Donut | Operating system share |
| 8 | Day of Week Activity | Column Chart | Peak traffic days |
| 9 | Session Depth | Column Chart | Events per user distribution |
| 10 | Scroll Rate | Scorecards + Donut | Content engagement depth |

---

## Key Insights

- **Traffic quality gap:** Google Organic drives the most volume (15 users) but the referral channel from `shop.googlemerchandisestore.com` delivers the highest engagement quality (71.4% vs 52.6%)
- **Shallow sessions:** 74% of users triggered only 1 event per session — a key UX signal worth investigating
- **High scroll rate:** 84.6% of sessions include a scroll event — users who stay do engage with content
- **Promotion CTR:** 14.3% click-through rate on promotions (industry benchmark: 2–5%)
- **Sunday peak:** Sunday generates the most sessions, suggesting weekend browsing behaviour
- **Desktop dominant:** 73% desktop, 76.3% Chrome — informs QA and design priorities

> **Data context:** GA4 Obfuscated Sample, January 2021. 52 events, 38 users. All percentages are directional only. No purchase events present in this slice. revenue metrics reflect $0 accurately.

---

## Tech Stack

| Layer | Tool |
|---|---|
| Language | Python 3.11 |
| Data processing | pandas, numpy |
| Google Sheets integration | gspread, gspread-dataframe, google-auth |
| Scheduling | GitHub Actions (cron) |
| Notebook | Google Colab |
| Dashboard | Looker Studio |
| Secrets management | GitHub Encrypted Secrets |

---

## 📄 License

Built for Comms8 data & automation intern assessment purposes. Dataset: [GA4 Obfuscated Sample E-Commerce](https://developers.google.com/analytics/bigquery/web-ecommerce-demo-dataset)
