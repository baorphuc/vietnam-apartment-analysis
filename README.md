# 🏙️ Vietnam Apartment Price Analysis

An end-to-end data analytics project scraping, processing, and visualizing apartment listing data from [batdongsan.com.vn](https://batdongsan.com.vn) across **Ho Chi Minh City** and **Hanoi**.

---

## 📌 Business Problem

> **Which districts in HCMC and Hanoi offer the best value for apartment buyers?**

Secondary questions:
- How do prices vary across districts and market segments?
- Which districts have the highest price volatility?
- What apartment sizes dominate each city's market?

---

## 🗂️ Project Structure

```
vietnam-apartment-analysis/
├── scraper/
│   ├── base.py              # Base scraper (curl_cffi, retry, sleep)
│   └── bds_scraper.py       # batdongsan.com.vn scraper
├── etl/
│   ├── transformer.py       # Parse price, area, district normalization
│   ├── quality.py           # Data quality checks + P99 outlier flagging
│   └── loader.py            # PostgreSQL insert, duplicate handling
├── analysis/
│   ├── eda.py               # Load → clean → feature engineering → EDA
│   └── charts.py            # Plotly chart functions
├── dashboard/
│   ├── app.py               # Streamlit dashboard
│   └── map.py               # Choropleth map
├── db/
│   ├── schema.sql           # PostgreSQL schema (4 tables + 2 views)
│   └── db.py                # DB connection helper
├── data/
│   └── geojson/             # District boundaries (HCMC + HN)
├── main.py                  # Pipeline entry point
└── requirements.txt
```

---

## ⚙️ ETL Pipeline

```
batdongsan.com.vn
    ↓ scraper/ (curl_cffi + BeautifulSoup)
RawListing (dataclass)
    ↓ etl/transformer.py
Cleaned dict (price parsed, district normalized, price/m² computed)
    ↓ etl/quality.py
Quality-flagged records (IQR outlier, area < 15m², missing fields)
    ↓ etl/loader.py
PostgreSQL: apartments, price_history, scrape_logs
    ↓ analysis/eda.py
Pandas DataFrame → EDA → Plotly charts + Streamlit dashboard
```

---

## 🗄️ Database Schema

| Table | Description |
|-------|-------------|
| `apartments` | Main listings table (valid + invalid with flags) |
| `district_mapping` | Raw → canonical district name normalization |
| `price_history` | Price change tracking per listing |
| `scrape_logs` | Per-run metadata (inserted, skipped, invalid counts) |

**Views:** `v_apartments_clean`, `v_district_summary`

---

## 📊 Key Insights

- **Ba Dinh (HN)** is the most expensive district at **190.9 tr/m²**
- **Go Vap (HCMC)** is the most affordable at **40.0 tr/m²**
- **Thu Duc City** has the highest price volatility (CV = 38.5%)
- **68.7%** of HN listings fall in the luxury segment (>80 tr/m²)
- **Medium apartments (50–80m²)** dominate HCMC; large (>80m²) dominate HN

---

## 🖥️ Dashboard

4-page Streamlit dashboard:

| Page | Content |
|------|---------|
| Overview | KPI metrics, market segmentation, size distribution |
| District Analysis | Affordability ranking, price volatility, box plots |
| Market Map | Interactive choropleth by district |
| Price Trends | Median price/m² trend by month |

---

## 🚀 Setup

```bash
# 1. Clone
git clone https://github.com/baorphuc/vietnam-apartment-analysis
cd vietnam-apartment-analysis

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure environment
cp env.example .env
# Edit .env: set DB_HOST, DB_NAME, DB_USER, DB_PASSWORD

# 4. Create database
psql -U postgres -c "CREATE DATABASE apartment_analytics;"

# 5. Run migration
python db/db.py

# 6. Run pipeline
python main.py --city both --pages 20

# 7. Launch dashboard
streamlit run dashboard/app.py
```

---

## 🛠️ Tech Stack

| Layer | Tools |
|-------|-------|
| Scraping | `curl_cffi`, `BeautifulSoup4` |
| Storage | `PostgreSQL`, `psycopg2` |
| Processing | `pandas`, `numpy` |
| Visualization | `plotly`, `streamlit` |
| ML (optional) | `scikit-learn` |

---

## 📁 Data Collection

- **Source:** batdongsan.com.vn (public listings)
- **Scope:** Căn hộ (apartments) only, HCMC + Hanoi
- **Volume:** ~8,000+ listings (ongoing)
- **Anti-block:** TLS fingerprint spoofing via `curl_cffi`, random delay 2–5s

---

## 🔮 Future Improvements

- Scheduled scraping (daily via cron/Task Scheduler)
- Price prediction model (linear regression → XGBoost)
- Ward-level granularity
- Hanoi post-merger district boundaries (2025)
