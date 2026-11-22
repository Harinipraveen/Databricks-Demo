# ⚡ Finnish Electricity Lakehouse

An end-to-end data engineering project on **Databricks**, built with Finnish national grid data from **Fingrid**. Raw CSV files are ingested, cleaned and modelled through a **medallion architecture** (landing → bronze → silver → gold) governed by **Unity Catalog**, producing analytics-ready tables for BI reporting.

The project is also my hands-on preparation for the **Databricks Certified Data Engineer Associate** exam.

---

## 📌 Business questions

- How do Finland's electricity **consumption** and **production** vary by hour, day and season?
- How much does **wind power** contribute to total production, and how volatile is it?
- When is Finland a **net importer** or a **net exporter** of electricity?

---

## 🏗️ Architecture

```
Fingrid Open Data (CSV, 2023-01 → 2026-11)
          │  manual upload
          ▼
┌──────────────────────────────────────────────┐
│ LANDING  electricity.landing.fingrid_raw     │  Unity Catalog Volume
│          /production  /consumption  /wind    │  one folder per dataset
└──────────────────────────────────────────────┘
          │  Auto Loader (cloudFiles, availableNow)
          ▼
┌──────────────────────────────────────────────┐
│ BRONZE   electricity.bronze.{production,     │  raw data, as delivered,
│          consumption, wind}                  │  + audit columns
└──────────────────────────────────────────────┘
          │  PySpark: cast, union, clean, dedupe
          ▼
┌──────────────────────────────────────────────┐
│ SILVER   electricity.silver.grid_observations│  one typed, deduplicated
│                                              │  long-format table
└──────────────────────────────────────────────┘
          │  Spark SQL: weighted aggregation, pivot
          ▼
┌──────────────────────────────────────────────┐
│ GOLD     electricity.gold.hourly_grid (table)│  BI-ready
│          electricity.gold.daily_grid  (view) │
└──────────────────────────────────────────────┘
          │
          ▼
      Power BI report
```

---

## 🧰 Tech stack

| Area | Tools |
|---|---|
| Platform | Databricks Free Edition (serverless compute) |
| Storage & governance | Delta Lake, Unity Catalog (catalog, schemas, volumes) |
| Ingestion | Auto Loader (`cloudFiles`), `read_files` |
| Transformation | PySpark, Spark SQL, window functions |
| Version control | Databricks Git folders + GitHub |
| Reporting | Power BI |

---

## 📂 Data source

**[Fingrid Open Data](https://data.fingrid.fi/en)**, the Finnish transmission system operator.

| Dataset | Fingrid ID | Unit | Resolution |
|---|---|---|---|
| Electricity production in Finland | 74 | MWh/h | 1 h until 13 Jun 2023, then 15 min |
| Electricity consumption in Finland | 124 | MWh/h | 1 h until 13 Jun 2023, then 15 min |
| Wind power generation | 75 | MWh/h | 1 h until 13 Jun 2023, then 15 min |

- **Period:** 1 Jan 2023 – 1 Nov 2026 (Finnish time)
- **Format:** CSV, semicolon-separated, one observation per row, timestamps in UTC
- **Volume:** ~122,000 rows per dataset

> Data licensed under [Creative Commons Attribution 4.0](https://creativecommons.org/licenses/by/4.0/). Source: Fingrid Oyj.

---

## 🥉 Bronze: ingestion (`01_bronze_ingest`)

- Raw files land in a **Unity Catalog Volume**, with one folder per dataset so each becomes its own table.
- **Auto Loader** loads each folder incrementally. A **checkpoint** records which files have been processed, so reruns only pick up new files.
- `trigger(availableNow=True)` runs the stream as a batch: it processes everything new, then stops, which keeps compute costs low.
- The schema is inferred and stored in a schema location, enabling **schema evolution**. All columns are kept as strings to stay faithful to the source.
- **Audit columns** are added to every row: `source_file` (from `_metadata.file_path`) and `ingested_at`.

---

## 🥈 Silver: cleaning & conforming (`02_silver_transform`)

All three bronze tables are combined into one long-format table, `grid_observations`:

| Step | Technique |
|---|---|
| Cast strings to `INT`, `TIMESTAMP`, `DOUBLE` | `cast()` |
| Tag each row with its measure | `F.lit()` |
| Stack the three datasets | `unionByName()` (column-name matching, safer than `union`) |
| Derive the interval length (60 / 15 min) | `unix_timestamp()` arithmetic → `resolution_min` |
| Add Finnish local time for reporting | `from_utc_timestamp(..., 'Europe/Helsinki')` |
| Remove nulls | `filter()` |
| Round stray seconds, then deduplicate deterministically | `date_trunc('minute')` + `row_number()` over a window |

**Design choices**

- **UTC is the system of record.** It's used for joins and keys, because Finnish local time has a skipped hour every March and a repeated hour every October.
- Silver keeps the **native grain** (1 h and 15 min). Aggregation happens in gold.

---

## 🥇 Gold: analytics-ready (`03_gold`)

### `gold.hourly_grid` (table)

One row per hour, with consumption, production and wind side by side, plus `net_import_mw`.

Mixed 60-, 30- and 15-minute rows are aggregated with a **duration-weighted average**:

```sql
SUM(value_mw * resolution_min) / SUM(resolution_min)
```

This means intervals of different lengths count correctly, and missing slots don't bias the average. `consumption_minutes` shows how complete each hour is (60 = full).

**Net import** = consumption − production. Positive means Finland imported power that hour; negative means it exported.

### `gold.daily_grid` (view)

Daily averages, peak consumption, net import, **wind share of production (%)** and `hours_in_day`, grouped by Finnish calendar day.

**View vs materialized view vs table:** the daily summary is a view because it's cheap to compute from the small hourly table and always up to date. For a dashboard with many users, a **materialized view** would store and incrementally refresh the result instead.

---

## 🔍 Data quality findings

Validating the data surfaced several real-world issues, all detected and handled in the pipeline:

| Issue | How it was found | Handling |
|---|---|---|
| **Same file ingested twice** after a landing folder was renamed (Auto Loader tracks files by path) | `source_file` audit column showed two paths for one file | Removed with a Delta `DELETE`; landing files are now treated as immutable |
| **Duplicate slots one second apart** (e.g. 14:45:00 and 14:45:01) in wind data, Jun 2024 | Gap analysis showed 0 and −14 minute "gaps" | Rounded to the minute; kept the clean row via `row_number()` |
| **~85–95 hours of missing 15-min slots per measure**, concentrated in Sep–Nov 2024 | Gap detection with `LEAD()` over time | Not imputed; the weighted average and `consumption_minutes` absorb and expose them |
| **One fully missing hour** on 2024-06-03 | `hours_in_day = 23` on a non-DST day | Documented |
| **30-minute overlap** at the hourly → 15-minute switch (13 Jun 2023) | Negative gap value | Documented |
| **Two 30-minute rows** (consumption 2025-05-08, wind 2024-11-15) | `resolution_min` profiling | Handled by duration weighting |

**DST check:** `daily_grid` correctly shows 23 hours on the last Sunday of March and 25 hours on the last Sunday of October each year.

---

## 📊 Sample insights

| Day (FI) | Avg consumption | Avg production | Wind share | Net import |
|---|---|---|---|---|
| 2026-10-28 | 10,277 MW | 10,572 MW | **54.2 %** | −295 MW (export) |
| 2026-10-31 | 9,003 MW | 7,309 MW | 18.3 % | +1,694 MW (import) |

Windy days turn Finland into a net exporter; calm days mean importing over 1.5 GW.

---

## 🗂️ Repository structure

```
Databricks-Demo/
├── 00_setup              # catalog, schemas, volume (SQL)
├── 01_bronze_ingest      # Auto Loader → bronze tables (Python)
├── 02_silver_transform   # cleaning, dedup, conforming (PySpark)
├── 03_gold               # hourly table + daily view (SQL)
└── README.md
```

---

## ▶️ How to run

1. Create a [Databricks Free Edition](https://www.databricks.com/learn/free-edition) workspace and clone this repo as a Git folder.
2. Run `00_setup` to create the catalog, schemas and volume.
3. Download the three datasets from [data.fingrid.fi](https://data.fingrid.fi/en) as CSV ("each observation on its own row", UTC) and upload each into its own folder in `/Volumes/electricity/landing/fingrid_raw/` (`production`, `consumption`, `wind`).
4. Run `01_bronze_ingest`, then `02_silver_transform`, then `03_gold`.

---

## 🛣️ Roadmap

- [ ] **Orchestration:** Lakeflow Job chaining bronze → silver → gold, with retries and a file-arrival trigger
- [ ] **Spot prices:** ingest day-ahead prices from the ENTSO-E Transparency Platform API (XML)
- [ ] **API ingestion:** replace manual downloads with the Fingrid REST API, with keys stored in Databricks secrets
- [ ] **Declarative pipelines:** rebuild silver/gold as Lakeflow Declarative Pipelines with data-quality expectations
- [ ] **CI/CD:** Declarative Automation Bundles (`databricks.yml`) with dev/prod targets, deployed via GitHub Actions
- [ ] **Governance:** grants, row filters and column masks in Unity Catalog
- [ ] **Optimization:** Liquid Clustering on the silver table
- [ ] **Validation:** compare calculated net import with Fingrid's measured cross-border flows (dataset 194)
- [ ] **Power BI report** on the gold layer

---

## 👩‍💻 Author

**Harini Praveen**, BI / Data Analyst, Espoo, Finland

