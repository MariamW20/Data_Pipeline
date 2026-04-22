"""
Step 6 (Bonus): Streamlit dashboard for interactive exploration.
Run with:  streamlit run scripts/06_dashboard.py
"""

import os
import json
import sqlite3
import pandas as pd
import streamlit as st
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from project_paths import DB_PATH

# ─── Config ───────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Global Patent Intelligence",
    page_icon="🔬",
    layout="wide",
)

# Dark styling override
st.markdown("""
<style>
    .main { background-color: #0f172a; }
    h1, h2, h3 { color: #38bdf8 !important; }
    .metric-container { background: #1e293b; border-radius: 8px; padding: 12px; }
</style>
""", unsafe_allow_html=True)


# ─── DB helper ────────────────────────────────────────────────────────────────
@st.cache_resource
def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


@st.cache_data
def query(sql: str, params: tuple | None = None) -> pd.DataFrame:
    return pd.read_sql_query(sql, get_conn(), params=params)


# ─── Header ───────────────────────────────────────────────────────────────────
st.title("🔬 Global Patent Intelligence Dashboard")
st.caption("PatentsView data — USPTO Granted Patents")

if not os.path.exists(DB_PATH):
    st.error(f"Database not found at `{DB_PATH}`.\n\nRun `03_load_database.py` first.")
    st.stop()

# ─── KPI Row ──────────────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)

total_patents = query("SELECT COUNT(*) AS n FROM patents").iloc[0]["n"]
total_inv     = query("SELECT COUNT(*) AS n FROM inventors").iloc[0]["n"]
total_comp    = query("SELECT COUNT(*) AS n FROM companies").iloc[0]["n"]
year_range    = query("SELECT MIN(year) AS y1, MAX(year) AS y2 FROM patents WHERE year IS NOT NULL").iloc[0]

col1.metric("Total Patents",   f"{total_patents:,}")
col2.metric("Unique Inventors", f"{total_inv:,}")
col3.metric("Companies",        f"{total_comp:,}")
col4.metric("Year Range",       f"{int(year_range['y1'])} – {int(year_range['y2'])}")

st.divider()

# ─── Tabs ─────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📈 Trends", "🏆 Top Inventors", "🏢 Top Companies",
    "🌍 Countries", "🔍 Search Patents"
])

# ── Tab 1: Trends ─────────────────────────────────────────────────────────────
with tab1:
    st.subheader("Patents Granted per Year")
    year_df = query("""
        SELECT year, COUNT(*) AS patent_count
        FROM patents
        WHERE year BETWEEN 1976 AND 2024
        GROUP BY year ORDER BY year
    """)
    st.line_chart(year_df.set_index("year")["patent_count"])

    st.subheader("Recent Decade Detail")
    recent = year_df[year_df["year"] >= year_df["year"].max() - 10]
    st.bar_chart(recent.set_index("year")["patent_count"])

# ── Tab 2: Top Inventors ──────────────────────────────────────────────────────
with tab2:
    n_inv = st.slider("Number of top inventors", 5, 50, 15)
    inv_df = query(f"""
        SELECT i.name AS inventor, i.country,
               COUNT(DISTINCT r.patent_id) AS patent_count
        FROM relationships r
        JOIN inventors i ON r.inventor_id = i.inventor_id
        GROUP BY r.inventor_id
        ORDER BY patent_count DESC
        LIMIT {n_inv}
    """)
    st.dataframe(inv_df, use_container_width=True)
    st.bar_chart(inv_df.set_index("inventor")["patent_count"])

# ── Tab 3: Top Companies ──────────────────────────────────────────────────────
with tab3:
    n_comp = st.slider("Number of top companies", 5, 50, 15)
    comp_df = query(f"""
        SELECT c.name AS company, c.country,
               COUNT(DISTINCT r.patent_id) AS patent_count
        FROM relationships r
        JOIN companies c ON r.company_id = c.company_id
        GROUP BY r.company_id
        ORDER BY patent_count DESC
        LIMIT {n_comp}
    """)
    st.dataframe(comp_df, use_container_width=True)
    st.bar_chart(comp_df.set_index("company")["patent_count"])

# ── Tab 4: Countries ──────────────────────────────────────────────────────────
with tab4:
    cty_df = query("""
        SELECT i.country, COUNT(DISTINCT r.patent_id) AS patent_count,
               ROUND(COUNT(DISTINCT r.patent_id)*100.0/(SELECT COUNT(*) FROM patents),2) AS pct
        FROM relationships r
        JOIN inventors i ON r.inventor_id = i.inventor_id
        WHERE i.country NOT IN ('Unknown','','XX')
        GROUP BY i.country
        ORDER BY patent_count DESC
        LIMIT 20
    """)
    st.dataframe(cty_df, use_container_width=True)
    st.bar_chart(cty_df.set_index("country")["patent_count"])

# ── Tab 5: Search Patents ─────────────────────────────────────────────────────
with tab5:
    st.subheader("Search Patent Titles")
    search = st.text_input("Enter keyword(s)", placeholder="e.g. machine learning")
    year_filter = st.slider("Year range", 1976, 2024, (2010, 2024))

    if search:
        results = query(
            """
            SELECT patent_id, title, year, abstract
            FROM patents
            WHERE title LIKE ?
              AND year BETWEEN ? AND ?
            ORDER BY year DESC
            LIMIT 100
            """,
            (f"%{search}%", int(year_filter[0]), int(year_filter[1])),
        )
        st.write(f"Found **{len(results):,}** patents (showing up to 100)")
        st.dataframe(results, use_container_width=True)
    else:
        st.info("Type a keyword above to search patents.")
