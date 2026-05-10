# Global Patent Intelligence Data Pipeline

A complete data engineering project that collects, cleans, stores and analyzes
real-world patent data from the USPTO PatentsView dataset.

---

## Project Structure

```
BigData_Individual/
|
|- 01_download_data.py
|- 02_clean_data.py
|- 03_load_database.py
|- 04_analyze.py
|- 05_visualize.py
|- 06_dashboard.py
|- project_paths.py            <- Shared path resolution
|- schema.sql
|- queries.sql
|- requirements.txt
|- optimize_db.py              <- Create database indexes
|- README.md
|- Data Pipeline Mini Project.pdf
|- PV_grant_data_dictionary.pdf 
|- raw/                        <- Raw TSV files (input)
|  |- g_location_disambiguated.tsv <- Location → country mapping source
|  |- g_patent_abstract.tsv    <- Patent abstract source
|- clean/                      <- Clean CSV files
|- reports/                    <- CSV/JSON/PNG charts
|- scripts/
|  |- train_company_velocity.py <- Train supervised ML model for filing predictions
|- models/                     <- Trained model artifacts
|  |- company_velocity_model.joblib <- Persisted RandomForest model
|  `- company_velocity_metadata.json <- Feature names, label encoder, metrics
`- patents.db                  <- SQLite database
```

---

## Quick Start (Windows)

### 1. Install Python
Download Python 3.10+ from https://www.python.org/downloads/
✅ Check "Add Python to PATH" during installation.

### 2. Clone / Download this repo
```bash
git clone https://github.com/MariamW20/Data_Pipeline.git
cd patent-pipeline
```

### 3. Create a virtual environment
```bash
python -m venv venv
venv\Scripts\activate
```

### 4. Install dependencies
```bash
pip install -r requirements.txt
```

### 5. Run the pipeline (in order)

```bash
# Step 1 — Download raw data from PatentsView (~2–5 GB total)
python 01_download_data.py

# Step 2 — Clean data with pandas
python 02_clean_data.py

# Step 3 — Load into SQLite
python 03_load_database.py

# Step 4 — Run analysis + generate reports (reads SQL from queries.sql)
python 04_analyze.py

# Step 5 — Generate charts
python 05_visualize.py

# (Optional) Step 6a — Create database indexes for faster queries
python optimize_db.py

# (Optional) Step 6b — Train supervised ML model for company filing predictions
python scripts/train_company_velocity.py

# Step 7 — Launch interactive dashboard
streamlit run 06_dashboard.py
```

---

## Database Schema

### `patents`
| Column | Type | Description |
|--------|------|-------------|
| patent_id | TEXT PK | USPTO patent number |
| title | TEXT | Patent title |
| abstract | TEXT | Patent abstract |
| filing_date | TEXT | Grant date (YYYY-MM-DD) |
| year | INTEGER | Grant year |
| patent_type | TEXT | Patent type from PatentsView |
| wipo_kind | TEXT | WIPO kind code |

### `inventors`
| Column | Type | Description |
|--------|------|-------------|
| inventor_id | TEXT PK | Disambiguated inventor ID |
| name | TEXT | Full name |
| country | TEXT | Country name resolved from location data (falls back to code/Unknown) |

### `companies`
| Column | Type | Description |
|--------|------|-------------|
| company_id | TEXT PK | Disambiguated assignee ID |
| name | TEXT | Organisation name |
| country | TEXT | Country name resolved from location data (falls back to code/Unknown) |

### `relationships`
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto-increment row ID |
| patent_id | TEXT FK | Links to patents |
| inventor_id | TEXT FK | Links to inventors |
| company_id | TEXT FK | Links to companies |

---

## SQL Queries

| # | Query | Description |
|---|-------|-------------|
| Q1 | Top Inventors | Most prolific inventors by patent count |
| Q2 | Top Companies | Companies owning the most patents |
| Q3 | Countries | Patent share by inventor country |
| Q4 | Yearly Trends | Patents granted per year (1976–2024) |
| Q5 | JOIN Query | Patents joined with inventors + companies |
| Q6 | CTE Query | Top companies per year (WITH statement) |
| Q7 | Ranking Query | Inventor ranking with window functions |

---

## Reports Generated

| File | Description |
|------|-------------|
| `top_inventors.csv` | Top 20 inventors with patent counts |
| `top_companies.csv` | Top 20 companies with patent counts |
| `country_trends.csv` | Patent share by country |
| `yearly_trends.csv` | Patents per year 1976–2024 |
| `patent_report.json` | Full summary in JSON format |
| `chart_yearly_trend.png` | Line chart — patents over time |
| `chart_top_companies.png` | Bar chart — top companies |
| `chart_country_share.png` | Horizontal bar chart — country distribution (with percentages) |
| `chart_top_inventors.png` | Bar chart — top inventors |

---

## Dashboard Features (06_dashboard.py)

The Streamlit dashboard provides an interactive, visually refined interface with:

### Visual Design
- **Dark theme** with custom gradient backgrounds and glass-morphism UI elements
- **Improved chart readability**: increased title padding (26px), refined subtitle placement, and contrast-optimized foreground colors
- **Consistent styling**: all charts use a unified `apply_chart_style()` function for visual cohesion

### 6 Main Tabs

1. **Trends** — Patent growth over time with rolling averages, decade grouping, and peak annotations
2. **Inventors** — Top inventors ranked by patent count, inventor productivity distribution
3. **Companies** — Top companies with Pareto pattern analysis (concentration metrics)
4. **Countries** — Geographic distribution with treemap visualization showing inventor productivity per country
5. **Explore** — Searchable patent database by keyword with time-series results
6. **Insights & Predictions** — Advanced analytics including:
   - **Filing Rate Forecast** (2025–2029): polynomial trend fitting with 95% confidence intervals
   - **Emerging Tech Analysis**: keyword-based identification of AI, blockchain, IoT, quantum, biotech, renewable energy patents
   - **Inventor Longevity Trends**: career span distribution and patents vs. experience correlation
   - **Company Patent Velocity Comparison**: side-by-side filing trends across top firms
   - **Data Quality Diagnostics**: missing data analysis, coverage breadth, recency metrics, and outlier detection
   - **Supervised ML Model** (RandomForest Regressor): predicts next-year company patent filings based on lagged counts, inventor history, and growth indicators

### Machine Learning Integration

- **Model**: Trained RandomForest (300 trees) on company-year panels (2012–2024)
- **Features**: lagged filings (1, 2+ years), inventor counts, growth rates, country code, year
- **Metrics**: Test R² = 0.964, RMSE ≈ 2.81, MAE ≈ 0.57
- **Output**: 2025 filing predictions for top 12 companies with summary metrics (avg change, max growth, growth count)
- **Artifacts**: Model saved as `models/company_velocity_model.joblib`; metadata in `models/company_velocity_metadata.json`

### Performance Optimizations
- **Caching**: expensive queries cached with `@st.cache_data(ttl=3600)` (e.g., filing trends, emerging tech keywords)
- **Database Indexing**: indexes on `patents.year`, relationship foreign keys, for sub-second query response
- **Streaming Architecture**: asynchronous chart rendering prevents UI blocking

---

## Country Mapping Notes

Country values are generated using this join path:

1. `g_inventor_disambiguated.tsv` and `g_assignee_disambiguated.tsv` provide `location_id`.
2. `g_location_disambiguated.tsv` provides `disambig_country` for each `location_id`.
3. The cleaner converts country codes (for example, `US`, `JP`) to readable country names where possible.

Why `Unknown` can still appear:

- Missing `location_id` in source rows.
- `location_id` not present in `g_location_disambiguated.tsv`.
- Empty country value in the matched location row.

Why code-like values may still appear occasionally:

- Some country identifiers are not recognized by the mapper and are kept as-is to avoid data loss.

Abstract values are generated using this path:

1. `g_patent.tsv` provides core patent metadata (`patent_id`, title, dates, type).
2. `g_patent_abstract.tsv` provides `patent_abstract`.
3. The cleaner joins both datasets on `patent_id`.

---
---

## 🔗 Data Source

- **PatentsView Bulk Data**: https://data.patentsview.org/
- **Data Dictionary**: PV_grant_data_dictionary.pdf 

---

## 👤 Author
Mariam Wambui· Course: Software Engineering · Year: 2026
