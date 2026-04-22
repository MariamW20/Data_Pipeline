"""
Step 6: Streamlit dashboard for interactive exploration.
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
    page_icon=":material/travel_explore:",
    layout="wide",
)

# Dark styling override
st.markdown("""
<style>
    .main { background-color: #0f172a; }
    h1, h2, h3 { color: #38bdf8 !important; }
    .metric-container { background: #1e293b; border-radius: 8px; padding: 12px; }
    .hero-title {
        display: flex;
        align-items: center;
        gap: 0.75rem;
        margin-bottom: 0.1rem;
    }
    .hero-title h1 {
        margin: 0;
        font-size: 2rem;
        color: #e2e8f0;
    }
    .icon-chip {
        width: 34px;
        height: 34px;
        border-radius: 10px;
        background: linear-gradient(135deg, #0ea5e9 0%, #22d3ee 100%);
        display: inline-flex;
        align-items: center;
        justify-content: center;
        box-shadow: 0 6px 16px rgba(14, 165, 233, 0.35);
    }
    .icon-chip svg {
        width: 20px;
        height: 20px;
        stroke: #0f172a;
        fill: none;
        stroke-width: 2;
        stroke-linecap: round;
        stroke-linejoin: round;
    }
    .section-head {
        display: flex;
        align-items: center;
        gap: 0.65rem;
        margin-bottom: 0.25rem;
    }
    .section-head h3 {
        margin: 0;
        color: #e2e8f0 !important;
        font-size: 1.1rem;
    }
</style>
""", unsafe_allow_html=True)

ICONS = {
    "dashboard": """<svg viewBox='0 0 24 24'><path d='M3 12h18'/><path d='M12 3v18'/><circle cx='12' cy='12' r='8'/></svg>""",
    "trends": """<svg viewBox='0 0 24 24'><polyline points='3 17 9 11 13 15 21 7'/><polyline points='14 7 21 7 21 14'/></svg>""",
    "inventors": """<svg viewBox='0 0 24 24'><circle cx='9' cy='8' r='3'/><path d='M3 20c0-3.3 2.7-6 6-6s6 2.7 6 6'/><path d='M16 11a3 3 0 1 0 0-6'/><path d='M21 20c0-2.3-1.3-4.2-3.2-5.2'/></svg>""",
    "companies": """<svg viewBox='0 0 24 24'><rect x='3' y='7' width='7' height='14'/><rect x='14' y='3' width='7' height='18'/><path d='M6.5 11h0'/><path d='M6.5 15h0'/><path d='M17.5 8h0'/><path d='M17.5 12h0'/><path d='M17.5 16h0'/></svg>""",
    "countries": """<svg viewBox='0 0 24 24'><circle cx='12' cy='12' r='9'/><path d='M3 12h18'/><path d='M12 3a15 15 0 0 1 0 18'/><path d='M12 3a15 15 0 0 0 0 18'/></svg>""",
    "search": """<svg viewBox='0 0 24 24'><circle cx='11' cy='11' r='7'/><line x1='21' y1='21' x2='16.65' y2='16.65'/></svg>""",
}


def section_header(title: str, icon_key: str) -> None:
    st.markdown(
        f"""
        <div class='section-head'>
            <span class='icon-chip'>{ICONS[icon_key]}</span>
            <h3>{title}</h3>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ─── DB helper ────────────────────────────────────────────────────────────────
@st.cache_resource
def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


@st.cache_data
def query(sql: str, params: tuple | None = None) -> pd.DataFrame:
    return pd.read_sql_query(sql, get_conn(), params=params)


# ─── Header ───────────────────────────────────────────────────────────────────
st.markdown(
    f"""
    <div class='hero-title'>
        <span class='icon-chip'>{ICONS['dashboard']}</span>
        <h1>Global Patent Intelligence Dashboard</h1>
    </div>
    """,
    unsafe_allow_html=True,
)
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
    "Trends", "Top Inventors", "Top Companies", "Countries", "Search Patents"
])

# ── Tab 1: Trends ─────────────────────────────────────────────────────────────
with tab1:
    section_header("Patents Granted per Year", "trends")
    year_df = query("""
        SELECT year, COUNT(*) AS patent_count
        FROM patents
        WHERE year BETWEEN 1976 AND 2024
        GROUP BY year ORDER BY year
    """)
    st.line_chart(year_df.set_index("year")["patent_count"])

    section_header("Recent Decade Detail", "trends")
    recent = year_df[year_df["year"] >= year_df["year"].max() - 10]
    st.bar_chart(recent.set_index("year")["patent_count"])

# ── Tab 2: Top Inventors ──────────────────────────────────────────────────────
with tab2:
    section_header("Top Inventors", "inventors")
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
    section_header("Top Companies", "companies")
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
    section_header("Countries", "countries")
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
    section_header("Search Patent Titles", "search")
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
