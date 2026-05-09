"""
Step 6: Streamlit dashboard for interactive exploration.
Run with:  streamlit run scripts/06_dashboard.py
"""

import os
import json
import sqlite3
import pandas as pd
import numpy as np
import streamlit as st
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import squarify
from scipy import stats
from sklearn.preprocessing import LabelEncoder
import joblib
from project_paths import DB_PATH

CHART_FG = "#f8fafc"
CHART_MUTED = "#94a3b8"
CHART_PALETTE = ["#0ea5a4", "#f59e0b", "#6366f1", "#a78bfa", "#fb7185", "#34d399", "#f97316", "#7c3aed"]

# ─── Config ───────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Global Patent Intelligence",
    page_icon=":material/travel_explore:",
    layout="wide",
)

# Dark styling override
st.markdown("""
<style>
    .stApp {
        background:
            radial-gradient(circle at 12% 18%, rgba(56, 189, 248, 0.15), transparent 22%),
            radial-gradient(circle at 86% 12%, rgba(244, 114, 182, 0.12), transparent 18%),
            radial-gradient(circle at 78% 82%, rgba(245, 158, 11, 0.10), transparent 20%),
            linear-gradient(180deg, #07111f 0%, #0b1627 42%, #0f172a 100%);
    }
    .main {
        background: transparent;
    }
    .block-container {
        max-width: 1440px;
        padding-top: 1.2rem;
        padding-bottom: 2rem;
    }
    h1, h2, h3, h4 {
        color: #e2e8f0 !important;
        letter-spacing: -0.02em;
    }
    p, li, label, .stCaption {
        color: #cbd5e1;
    }
    .stMetric {
        background: rgba(15, 23, 42, 0.72);
        border: 1px solid rgba(148, 163, 184, 0.16);
        border-radius: 18px;
        padding: 0.9rem 1rem;
        box-shadow: 0 18px 40px rgba(2, 6, 23, 0.28);
        backdrop-filter: blur(16px);
    }
    .stMetric [data-testid="stMetricLabel"] {
        color: #94a3b8;
        font-size: 0.82rem;
    }
    .stMetric [data-testid="stMetricValue"] {
        color: #f8fafc;
        font-size: 1.7rem;
        font-weight: 700;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 0.5rem;
        background: rgba(15, 23, 42, 0.5);
        border: 1px solid rgba(148, 163, 184, 0.14);
        padding: 0.35rem;
        border-radius: 18px;
        backdrop-filter: blur(16px);
    }
    .stTabs [data-baseweb="tab"] {
        background: transparent;
        border-radius: 14px;
        color: #94a3b8;
        font-weight: 600;
        padding: 0.7rem 1rem;
    }
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, rgba(56, 189, 248, 0.28), rgba(244, 114, 182, 0.18));
        color: #f8fafc;
        border: 1px solid rgba(125, 211, 252, 0.26);
        box-shadow: 0 10px 24px rgba(14, 165, 233, 0.15);
    }
    .hero-shell {
        position: relative;
        overflow: hidden;
        border-radius: 28px;
        border: 1px solid rgba(148, 163, 184, 0.14);
        background:
            linear-gradient(135deg, rgba(8, 15, 30, 0.96), rgba(17, 24, 39, 0.92)),
            radial-gradient(circle at top left, rgba(56, 189, 248, 0.18), transparent 30%),
            radial-gradient(circle at bottom right, rgba(244, 114, 182, 0.12), transparent 26%);
        box-shadow: 0 24px 60px rgba(2, 6, 23, 0.36);
        padding: 1.4rem 1.5rem 1.2rem 1.5rem;
        margin-bottom: 1rem;
    }
    .hero-shell::after {
        content: "";
        position: absolute;
        inset: 0;
        background: linear-gradient(120deg, rgba(255,255,255,0.06), transparent 40%, transparent 60%, rgba(255,255,255,0.04));
        pointer-events: none;
    }
    .hero-title {
        display: flex;
        align-items: center;
        gap: 0.85rem;
        margin-bottom: 0.55rem;
    }
    .hero-title h1 {
        margin: 0;
        font-size: 2.15rem;
        font-weight: 800;
        line-height: 1.1;
        color: #f8fafc;
    }
    .hero-copy {
        max-width: 72ch;
        color: #cbd5e1;
        line-height: 1.65;
        margin-bottom: 1rem;
    }
    .hero-badges {
        display: flex;
        flex-wrap: wrap;
        gap: 0.6rem;
    }
    .badge {
        display: inline-flex;
        align-items: center;
        gap: 0.45rem;
        padding: 0.45rem 0.8rem;
        border-radius: 999px;
        border: 1px solid rgba(148, 163, 184, 0.18);
        background: rgba(15, 23, 42, 0.6);
        color: #dbeafe;
        font-size: 0.84rem;
        box-shadow: 0 10px 26px rgba(2, 6, 23, 0.22);
    }
    .badge strong {
        color: #f8fafc;
    }
    .icon-chip {
        width: 34px;
        height: 34px;
        border-radius: 10px;
        background: linear-gradient(135deg, #0ea5e9 0%, #22d3ee 52%, #f472b6 100%);
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
        margin: 0.25rem 0 0.55rem 0;
    }
    .section-head h3 {
        margin: 0;
        color: #f8fafc !important;
        font-size: 1.12rem;
    }
    .visual-panel {
        padding: 1rem 1rem 0.5rem 1rem;
        border-radius: 22px;
        border: 1px solid rgba(148, 163, 184, 0.12);
        background: rgba(15, 23, 42, 0.42);
        box-shadow: 0 18px 44px rgba(2, 6, 23, 0.2);
        margin-bottom: 0.9rem;
    }
    .insight-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        gap: 0.75rem;
        margin: 0.25rem 0 0.9rem 0;
    }
    .insight-card {
        padding: 0.9rem 1rem;
        border-radius: 18px;
        background: linear-gradient(180deg, rgba(15, 23, 42, 0.95), rgba(17, 24, 39, 0.78));
        border: 1px solid rgba(148, 163, 184, 0.14);
        box-shadow: 0 14px 34px rgba(2, 6, 23, 0.18);
    }
    .insight-card .label {
        font-size: 0.78rem;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: #94a3b8;
        margin-bottom: 0.3rem;
    }
    .insight-card .value {
        font-size: 1.3rem;
        font-weight: 800;
        color: #f8fafc;
        margin-bottom: 0.18rem;
    }
    .insight-card .note {
        color: #cbd5e1;
        font-size: 0.88rem;
    }
    .stSubheader {
        color: #f8fafc !important;
        margin-top: 0.8rem;
        margin-bottom: 0.6rem;
    }
    .stCaption {
        color: #cbd5e1 !important;
        background: transparent !important;
    }
    .stInfo, .stWarning, .stSuccess, .stError {
        background: rgba(15, 23, 42, 0.6) !important;
        border: 1px solid rgba(148, 163, 184, 0.14) !important;
        border-radius: 12px !important;
    }
    .stDivider {
        background: linear-gradient(90deg, transparent, rgba(148, 163, 184, 0.2), transparent) !important;
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
    <div class='hero-shell'>
        <div class='hero-title'>
            <span class='icon-chip'>{ICONS['dashboard']}</span>
            <h1>Global Patent Intelligence Dashboard</h1>
        </div>
        <div class='hero-copy'>
            Explore the patent dataset through a more editorial, insight-led interface: trend lines, concentration patterns,
            geographic distribution, and searchable abstracts are surfaced as visual summaries rather than raw tables.
        </div>
        <div class='hero-badges'>
            <span class='badge'><strong>Dataset:</strong> PatentsView USPTO grants</span>
            <span class='badge'><strong>Focus:</strong> visual analytics</span>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

if not os.path.exists(DB_PATH):
    st.error(f"Database not found at `{DB_PATH}`.\n\nRun `03_load_database.py` first.")
    st.stop()

# ─── KPI Row ──────────────────────────────────────────────────────────────────
def scalar(sql: str, params: tuple | None = None):
    return query(sql, params).iloc[0, 0]


def plot_horizontal_bar(df: pd.DataFrame, label_col: str, value_col: str, title: str, color: str, subtitle: str = "") -> None:
    if df.empty:
        st.info(f"No data available for {title.lower()}.")
        return

    data = df.copy()
    data[value_col] = pd.to_numeric(data[value_col], errors="coerce")
    data = data.dropna(subset=[value_col]).sort_values(value_col)
    if data.empty:
        st.info(f"No numeric data available for {title.lower()}.")
        return

    fig, ax = plt.subplots(figsize=(10.5, max(4.8, 0.42 * len(data) + 1.6)))
    bar_colors = [CHART_PALETTE[i % len(CHART_PALETTE)] for i in range(len(data))]
    bars = ax.barh(data[label_col], data[value_col], color=bar_colors, height=0.68, edgecolor="#0b1220", linewidth=0.8)
    apply_chart_style(ax, title, subtitle)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    for bar, val in zip(bars, data[value_col]):
        val_num = float(val)
        ax.text(
            bar.get_width() + max(data[value_col].max() * 0.012, 0.18),
            bar.get_y() + bar.get_height() / 2,
            f"{int(val_num):,}",
            va="center",
            fontsize=8.5,
            color=CHART_FG,
        )
    plt.tight_layout()
    st.pyplot(fig, clear_figure=True)


def plot_line_trend(df: pd.DataFrame, x_col: str, y_col: str, title: str, color: str, subtitle: str = "") -> None:
    if df.empty:
        st.info(f"No data available for {title.lower()}.")
        return

    data = df.copy()
    data[x_col] = pd.to_numeric(data[x_col], errors="coerce")
    data[y_col] = pd.to_numeric(data[y_col], errors="coerce")
    data = data.dropna(subset=[x_col, y_col]).sort_values(x_col)
    if data.empty:
        st.info(f"No numeric data available for {title.lower()}.")
        return

    data["rolling"] = data[y_col].rolling(5, min_periods=1).mean()

    fig, ax = plt.subplots(figsize=(11.5, 5.4))
    ax.fill_between(data[x_col], data[y_col], alpha=0.20, color=color)
    ax.plot(data[x_col], data[y_col], color=color, linewidth=2.6, label="Annual count")
    ax.plot(data[x_col], data["rolling"], color="#f472b6", linewidth=2.2, linestyle="--", label="5-year rolling avg")
    apply_chart_style(ax, title, subtitle)
    ax.set_xlabel("Year")
    ax.set_ylabel("Patents")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.legend(frameon=False, labelcolor=CHART_FG, loc="upper left")
    if len(data) > 2:
        peak = data.loc[data[y_col].idxmax()]
        ax.scatter([peak[x_col]], [peak[y_col]], s=85, color="#f59e0b", zorder=5, edgecolors="#0b1220", linewidth=1)
        ax.annotate(
            f"Peak {int(peak[x_col])}",
            (peak[x_col], peak[y_col]),
            textcoords="offset points",
            xytext=(10, 10),
            fontsize=8.5,
            color=CHART_FG,
            bbox=dict(boxstyle="round,pad=0.25", fc="#0b1220", ec="#334155", alpha=0.95),
        )
    plt.tight_layout()
    st.pyplot(fig, clear_figure=True)


def plot_pie(df: pd.DataFrame, label_col: str, value_col: str, title: str) -> None:
    # Replaced pie with a labeled horizontal bar fallback for accessibility and consistent styling
    if df.empty:
        st.info(f"No data available for {title.lower()}.")
        return

    data = df.copy()
    data[value_col] = pd.to_numeric(data[value_col], errors="coerce")
    data = data.dropna(subset=[value_col])
    if data.empty:
        st.info(f"No numeric data available for {title.lower()}.")
        return

    data = data.sort_values(value_col, ascending=True)
    fig, ax = plt.subplots(figsize=(8.6, max(4, 0.35 * len(data) + 1.6)))
    colors = [CHART_PALETTE[i % len(CHART_PALETTE)] for i in range(len(data))]
    ax.barh(data[label_col], data[value_col], color=colors, edgecolor="#0b1220", height=0.7)

    total = data[value_col].sum()
    for i, (val, lbl) in enumerate(zip(data[value_col], data[label_col])):
        pct = (val / total * 100) if total else 0
        ax.text(val + max(total * 0.01, 1), i, f"{int(val):,} ({pct:.1f}%)", va="center", color=CHART_FG, fontsize=9)

    apply_chart_style(ax, title, "")
    plt.tight_layout()
    st.pyplot(fig, clear_figure=True)


def summarize_text(text: str | None, limit: int = 240) -> str:
    if not text:
        return "No abstract available."
    clean = " ".join(str(text).split())
    return clean if len(clean) <= limit else clean[: limit - 1].rstrip() + "…"


def apply_chart_style(ax, title: str, subtitle: str = "") -> None:
    ax.set_facecolor("#0b1220")
    ax.figure.set_facecolor("#0b1220")
    ax.set_title(title, fontsize=16, pad=26, color=CHART_FG, fontweight="bold")
    if subtitle:
        ax.text(0, 1.12, subtitle, transform=ax.transAxes, fontsize=9.5, color=CHART_MUTED)
    ax.tick_params(colors=CHART_MUTED, labelsize=9)
    for side in ["top", "right"]:
        ax.spines[side].set_visible(False)
    for side in ["left", "bottom"]:
        ax.spines[side].set_color("#334155")
    ax.grid(True, alpha=0.22, color="#334155")
    ax.set_axisbelow(True)


col1, col2, col3, col4, col5 = st.columns(5)

total_patents = int(scalar("SELECT COUNT(*) FROM patents"))
total_inv = int(scalar("SELECT COUNT(*) FROM inventors"))
total_comp = int(scalar("SELECT COUNT(*) FROM companies"))
active_countries = int(scalar("SELECT COUNT(DISTINCT country) FROM inventors WHERE country NOT IN ('Unknown','','XX')"))
year_range = query("SELECT MIN(year) AS y1, MAX(year) AS y2 FROM patents WHERE year IS NOT NULL").iloc[0]
top_inventor = query("""
    SELECT i.name AS inventor, i.country, COUNT(DISTINCT r.patent_id) AS patent_count
    FROM relationships r
    JOIN inventors i ON r.inventor_id = i.inventor_id
    GROUP BY r.inventor_id
    ORDER BY patent_count DESC
    LIMIT 1
""").iloc[0]
top_company = query("""
    SELECT c.name AS company, c.country, COUNT(DISTINCT r.patent_id) AS patent_count
    FROM relationships r
    JOIN companies c ON r.company_id = c.company_id
    GROUP BY r.company_id
    ORDER BY patent_count DESC
    LIMIT 1
""").iloc[0]

col1.metric("Total Patents", f"{total_patents:,}")
col2.metric("Unique Inventors", f"{total_inv:,}")
col3.metric("Companies", f"{total_comp:,}")
col4.metric("Active Countries", f"{active_countries:,}")
col5.metric("Year Range", f"{int(year_range['y1'])} – {int(year_range['y2'])}")

st.divider()

st.info(
    f"Highest inventor count: {top_inventor['inventor']} ({int(top_inventor['patent_count']):,} patents). "
    f"Highest company count: {top_company['company']} ({int(top_company['patent_count']):,} patents)."
)

st.markdown(
    """
    <div class='insight-grid'>
        <div class='insight-card'>
            <div class='label'>Trend lens</div>
            <div class='value'>Growth over time</div>
            <div class='note'>See long-run changes with rolling averages and decade grouping.</div>
        </div>
        <div class='insight-card'>
            <div class='label'>Concentration lens</div>
            <div class='value'>Who dominates</div>
            <div class='note'>Top inventors and companies are highlighted with Pareto-style views.</div>
        </div>
        <div class='insight-card'>
            <div class='label'>Geography lens</div>
            <div class='value'>Where innovation clusters</div>
            <div class='note'>Country distribution is shown as both bars and share-based pies.</div>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# ─── Tabs ─────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Trends", "Inventors", "Companies", "Countries", "Explore", "Insights & Predictions"
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
    trend_col, insight_col = st.columns([2, 1])
    with trend_col:
        plot_line_trend(
            year_df,
            "year",
            "patent_count",
            "Patent Growth Over Time",
            "#38bdf8",
            "Annual filings plus a 5-year rolling average reveal the long-term trajectory.",
        )
    with insight_col:
        st.markdown("<h3 style='margin-top: 0; color: #f8fafc;'>Trend insights</h3>", unsafe_allow_html=True)
        if not year_df.empty:
            peak_row = year_df.loc[year_df["patent_count"].idxmax()]
            first_avg = year_df.head(5)["patent_count"].mean()
            last_avg = year_df.tail(5)["patent_count"].mean()
            growth = ((last_avg - first_avg) / first_avg * 100) if first_avg else 0
            st.metric("Peak filing year", f"{int(peak_row['year'])} ({int(peak_row['patent_count']):,})")
            st.metric("5-year growth", f"{growth:.1f}%")
            st.write("This view highlights acceleration, plateau periods, and major shifts in filing behavior.")
        else:
            st.info("No yearly trend data available.")

    st.markdown("---")
    section_header("Patent Activity by Decade", "trends")
    decade_df = query("""
        SELECT (year / 10) * 10 AS decade, COUNT(*) AS patent_count
        FROM patents
        WHERE year BETWEEN 1976 AND 2024
        GROUP BY decade
        ORDER BY decade
    """)
    plot_horizontal_bar(
        decade_df,
        "decade",
        "patent_count",
        "Filings by Decade",
        "#f472b6",
        "Decade grouping exposes long-run concentration in patent output.",
    )

# ── Tab 2: Inventors ───────────────────────────────────────────────────────────
with tab2:
    section_header("Top Inventors", "inventors")
    n_inv = st.slider("Number of top inventors", 5, 30, 15)
    inv_df = query(f"""
        SELECT i.name AS inventor, i.country,
               COUNT(DISTINCT r.patent_id) AS patent_count
        FROM relationships r
        JOIN inventors i ON r.inventor_id = i.inventor_id
        GROUP BY r.inventor_id
        ORDER BY patent_count DESC
        LIMIT {n_inv}
    """)
    inv_col, dist_col = st.columns([2, 1])
    with inv_col:
        plot_horizontal_bar(
            inv_df.assign(label=inv_df["inventor"] + "  [" + inv_df["country"] + "]"),
            "label",
            "patent_count",
            "Most Productive Inventors",
            "#38bdf8",
            "Ranks inventors by distinct patent count.",
        )
    with dist_col:
        st.markdown("<h3 style='margin-top: 0; color: #f8fafc;'>Inventor concentration</h3>", unsafe_allow_html=True)
        prod_df = query("""
            SELECT patent_count, COUNT(*) AS inventor_count
            FROM (
                SELECT inventor_id, COUNT(DISTINCT patent_id) AS patent_count
                FROM relationships
                GROUP BY inventor_id
            )
            GROUP BY patent_count
            ORDER BY patent_count
        """)
        if not prod_df.empty:
            fig, ax = plt.subplots(figsize=(7.2, 4.8))
            ax.bar(prod_df["patent_count"], prod_df["inventor_count"], color="#f472b6")
            ax.set_title("Inventor Productivity Distribution", fontsize=14, pad=10, color="#e2e8f0")
            ax.set_xlabel("Patents per inventor")
            ax.set_ylabel("Inventor count")
            ax.grid(True, axis="y", alpha=0.25)
            ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))
            ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
            plt.tight_layout()
            st.pyplot(fig, clear_figure=True)
            mode_bucket = prod_df.sort_values(["inventor_count", "patent_count"], ascending=[False, True]).iloc[0]
            st.caption(
                f"Most inventors cluster around {int(mode_bucket['patent_count'])} patents each, which shows a long-tail productivity pattern."
            )
        else:
            st.info("No inventor distribution data available.")

# ── Tab 3: Companies ──────────────────────────────────────────────────────────
with tab3:
    section_header("Top Companies", "companies")
    n_comp = st.slider("Number of top companies", 5, 30, 15)
    comp_df = query(f"""
        SELECT c.name AS company, c.country,
               COUNT(DISTINCT r.patent_id) AS patent_count
        FROM relationships r
        JOIN companies c ON r.company_id = c.company_id
        GROUP BY r.company_id
        ORDER BY patent_count DESC
        LIMIT {n_comp}
    """)
    comp_col, pareto_col = st.columns([2, 1])
    with comp_col:
        plot_horizontal_bar(
            comp_df.assign(label=comp_df["company"] + "  [" + comp_df["country"] + "]"),
            "label",
            "patent_count",
            "Top Companies by Patent Count",
            "#0ea5e9",
            "The bar length reflects distinct patents linked to each company.",
        )
    with pareto_col:
        st.markdown("<h3 style='margin-top: 0; color: #f8fafc;'>Concentration view</h3>", unsafe_allow_html=True)
        pareto = comp_df.copy()
        pareto["patent_count"] = pd.to_numeric(pareto["patent_count"], errors="coerce")
        pareto = pareto.dropna(subset=["patent_count"]).sort_values("patent_count", ascending=False)
        if not pareto.empty:
            pareto["cum_share"] = pareto["patent_count"].cumsum() / pareto["patent_count"].sum() * 100
            fig, ax1 = plt.subplots(figsize=(8.4, 6.2))
            ax1.bar(range(len(pareto)), pareto["patent_count"], color="#38bdf8", edgecolor="#0b1220", linewidth=0.7)
            ax1.set_facecolor("#111827")
            fig.set_facecolor("#111827")
            ax1.set_title("Company Pareto Pattern", fontsize=14, pad=22, color=CHART_FG, fontweight="bold")
            ax1.set_xlabel("Company rank", color=CHART_MUTED)
            ax1.set_ylabel("Patents", color=CHART_MUTED)
            ax1.tick_params(colors=CHART_MUTED, labelsize=8)
            ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
            ax1.grid(True, axis="y", alpha=0.2, color="#334155")
            for side in ["top", "right"]:
                ax1.spines[side].set_visible(False)
            for side in ["left", "bottom"]:
                ax1.spines[side].set_color("#334155")
            ax2 = ax1.twinx()
            ax2.plot(range(len(pareto)), pareto["cum_share"], color="#f472b6", marker="o", linewidth=2.4, markersize=4)
            ax2.set_ylabel("Cumulative share (%)", color=CHART_MUTED)
            ax2.tick_params(colors=CHART_MUTED, labelsize=8)
            ax2.set_ylim(0, 105)
            ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x)}%"))
            ax2.spines["right"].set_color("#334155")
            plt.tight_layout()
            st.pyplot(fig, clear_figure=True)
            top10_share = pareto.head(min(10, len(pareto)))
            share_value = top10_share["patent_count"].sum() / pareto["patent_count"].sum() * 100
            st.caption(f"Top {len(top10_share)} companies account for {share_value:.1f}% of the selected company set.")
        else:
            st.info("No company concentration data available.")

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
    country_col, pie_col = st.columns([2, 1])
    with country_col:
        plot_horizontal_bar(
            cty_df,
            "country",
            "patent_count",
            "Patent Share by Country",
            "#f59e0b",
            "Country-level distribution exposes geographic concentration in the dataset.",
        )
    with pie_col:
        st.markdown("<h3 style='margin-top: 0; color: #f8fafc;'>Inventor Productivity</h3>", unsafe_allow_html=True)
        # Get inventor metrics by country
        inventor_metrics = query("""
            SELECT i.country,
                   COUNT(DISTINCT i.inventor_id) AS inventor_count,
                   COUNT(DISTINCT r.patent_id) AS total_patents,
                   ROUND(CAST(COUNT(DISTINCT r.patent_id) AS FLOAT) / COUNT(DISTINCT i.inventor_id), 2) AS avg_patents_per_inventor
            FROM relationships r
            JOIN inventors i ON r.inventor_id = i.inventor_id
            WHERE i.country NOT IN ('Unknown','','XX')
            GROUP BY i.country
            ORDER BY total_patents DESC
            LIMIT 12
        """)
        
        if not inventor_metrics.empty:
            # Treemap: area -> total_patents; color -> avg_patents_per_inventor
            fig, ax = plt.subplots(figsize=(9.2, 6.8))

            sizes = inventor_metrics['total_patents'].astype(float).tolist()
            avg_vals = inventor_metrics['avg_patents_per_inventor'].astype(float).tolist()
            countries = inventor_metrics['country'].tolist()

            # Create labels (shorten country names)
            labels = []
            for c, tp, av in zip(countries, sizes, avg_vals):
                short = c if len(c) <= 18 else c[:15] + '...'
                labels.append(f"{short}\n{int(tp)} patents\n{av} avg")

            # Map avg_patents_per_inventor to a colormap
            norm = plt.Normalize(min(avg_vals), max(avg_vals)) if len(avg_vals) > 1 else plt.Normalize(0, max(avg_vals or [1]))
            cmap = plt.cm.get_cmap('plasma')
            colors = [cmap(norm(v)) for v in avg_vals]

            # Plot treemap
            squarify.plot(sizes=sizes, label=labels, color=colors, alpha=0.95, pad=True, ax=ax)
            ax.axis('off')

            ax.set_title("Inventor Productivity by Country (Treemap)", fontsize=14, pad=16, color=CHART_FG, fontweight='bold')
            fig.set_facecolor("#111827")

            # Add colorbar legend for avg patents per inventor
            sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
            sm.set_array([])
            cbar = fig.colorbar(sm, ax=ax, fraction=0.04, pad=0.01)
            cbar.ax.yaxis.set_tick_params(color=CHART_MUTED)
            plt.setp(plt.getp(cbar.ax.axes, 'yticklabels'), color=CHART_MUTED)

            plt.tight_layout()
            st.pyplot(fig, clear_figure=True)

            st.caption("💡 Treemap area shows total patents; color indicates avg patents per inventor.")
        else:
            st.info("No inventor productivity data available.")

# ── Tab 5: Explore ────────────────────────────────────────────────────────────
with tab5:
    section_header("Search Patent Titles", "search")
    search = st.text_input("Enter keyword(s)", placeholder="e.g. machine learning")
    year_filter = st.slider("Year range", 1976, 2024, (2010, 2024))

    if search:
        results = query(
            """
            SELECT patent_id, title, year, abstract
            FROM patents
            WHERE (title LIKE ? OR abstract LIKE ?)
              AND year BETWEEN ? AND ?
            ORDER BY year DESC
            LIMIT 100
            """,
            (f"%{search}%", f"%{search}%", int(year_filter[0]), int(year_filter[1])),
        )
        st.write(f"Found **{len(results):,}** patents (showing the strongest matches and a time-based summary)")

        if not results.empty:
            results_years = results.groupby("year").size().reset_index(name="patent_count")
            plot_line_trend(
                results_years,
                "year",
                "patent_count",
                f"Search Results Over Time: {search}",
                "#22d3ee",
                "Search matches by year show when the topic gained traction.",
            )

            st.markdown("#### Top matching patents")
            for _, row in results.head(10).iterrows():
                year_text = int(row["year"]) if pd.notna(row["year"]) else "N/A"
                with st.expander(f"{year_text} · {row['title']}"):
                    st.write(f"Patent ID: {row['patent_id']}")
                    st.write(summarize_text(row["abstract"]))
        else:
            st.info("No matching patents found for the selected filters.")
    else:
        st.info("Type a keyword to explore patents through title and abstract matches.")


# ── Tab 6: Insights & Predictions ─────────────────────────────────────────────
with tab6:
    section_header("Advanced Analytics & Predictions", "trends")
    st.markdown(
        """
        <div class='insight-card' style='background: linear-gradient(135deg, rgba(56, 189, 248, 0.14), rgba(244, 114, 182, 0.1)); margin-bottom: 1.2rem;'>
            <div class='label'>Predictive Module</div>
            <div style='color: #f8fafc; line-height: 1.5;'>
                This section applies statistical forecasting techniques to historical patent data, computing confidence-bounded
                projections of future filing activity, emerging technology areas, and inventor productivity trends.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Subheader for filing trend forecasts
    st.markdown("<h3 style='margin-top: 1.2rem; color: #f8fafc;'>Filing Rate Forecast</h3>", unsafe_allow_html=True)
    
    @st.cache_data(ttl=3600)
    def get_filing_trend():
        return query("""
            SELECT year, COUNT(*) AS patent_count
            FROM patents
            WHERE year BETWEEN 1976 AND 2024
            GROUP BY year ORDER BY year
        """)
    
    year_df = get_filing_trend()
    
    if not year_df.empty:
        from numpy.polynomial import Polynomial
        
        # Linear trend fit
        X = year_df["year"].values
        y = year_df["patent_count"].values
        
        # Fit polynomial (degree 2 for smoothness)
        coef = Polynomial.fit(X, y, 2).convert().coef
        p = Polynomial(coef)
        
        # Forecast next 5 years
        future_years = np.array([2025, 2026, 2027, 2028, 2029])
        future_pred = p(future_years)
        future_pred = np.maximum(future_pred, 0)  # No negative values
        
        # Calculate trend line for current data
        trend_pred = p(X)
        residuals = y - trend_pred
        std_err = np.std(residuals)
        
        # Create forecast dataframe
        forecast_df = pd.DataFrame({
            "year": future_years,
            "predicted": future_pred,
            "lower_bound": future_pred - 1.96 * std_err,
            "upper_bound": future_pred + 1.96 * std_err,
        })
        
        # Plot historical + forecast
        fig, ax = plt.subplots(figsize=(11.5, 5.6))
        
        # Historical
        ax.fill_between(X, y, alpha=0.15, color="#38bdf8")
        ax.plot(X, y, color="#38bdf8", linewidth=2.4, label="Historical filings")
        ax.plot(X, trend_pred, color="#f472b6", linewidth=2, linestyle="--", label="Trend (polynomial fit)")
        
        # Forecast
        all_years = np.concatenate([X, future_years])
        all_pred = np.concatenate([trend_pred, forecast_df["predicted"].values])
        ax.plot(future_years, forecast_df["predicted"], color="#f59e0b", linewidth=2.4, label="2025–2029 forecast")
        ax.fill_between(
            future_years,
            forecast_df["lower_bound"],
            forecast_df["upper_bound"],
            alpha=0.18,
            color="#f59e0b",
            label="95% confidence interval",
        )
        
        # Styling
        ax.set_facecolor("#111827")
        fig.set_facecolor("#111827")
        ax.set_title("Patent Filing Forecast (2025–2029)", fontsize=15, pad=20, color=CHART_FG, fontweight="bold")
        ax.set_xlabel("Year", color=CHART_MUTED)
        ax.set_ylabel("Patents", color=CHART_MUTED)
        ax.tick_params(colors=CHART_MUTED, labelsize=9)
        for side in ["top", "right"]:
            ax.spines[side].set_visible(False)
        for side in ["left", "bottom"]:
            ax.spines[side].set_color("#334155")
        ax.grid(True, alpha=0.22, color="#334155")
        ax.set_axisbelow(True)
        ax.legend(frameon=False, labelcolor=CHART_FG, loc="upper left")
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
        
        plt.tight_layout()
        st.pyplot(fig, clear_figure=True)
        
        # Forecast details
        forecast_col1, forecast_col2, forecast_col3 = st.columns(3)
        avg_forecast = forecast_df["predicted"].mean()
        last_actual = y[-1]
        forecast_change = ((avg_forecast - last_actual) / last_actual * 100) if last_actual else 0
        
        with forecast_col1:
            st.metric("Avg. forecast 2025–2029", f"{int(avg_forecast):,}")
        with forecast_col2:
            st.metric("Latest actual (2024)", f"{int(last_actual):,}")
        with forecast_col3:
            st.metric("Projected trend", f"{forecast_change:+.1f}%")
        
        st.caption(
            f"⚠️ Forecast assumes continuation of historical trend. Confidence interval widens beyond data range, reflecting uncertainty."
        )
    else:
        st.info("No historical data available for forecasting.")

    st.divider()

    # Emerging technology areas (based on recent title patterns)
    st.markdown("<h3 style='margin-top: 1.2rem; color: #f8fafc;'>Emerging Technology Areas</h3>", unsafe_allow_html=True)
    
    @st.cache_data(ttl=3600)
    def get_emerging_tech():
        tech_keywords = {
            "AI/Machine Learning": ["machine learning", "neural", "artificial intelligence", "ai", "learning model"],
            "Blockchain": ["blockchain", "distributed ledger", "cryptocurrency", "smart contract"],
            "IoT": ["internet of things", "iot", "sensor network", "connected device"],
            "Quantum": ["quantum", "qubit", "quantum computing"],
            "Biotechnology": ["biotech", "gene", "dna", "protein", "crispr"],
            "Renewable Energy": ["solar", "wind", "renewable", "battery", "energy storage"],
        }
        
        tech_recent = {}
        for tech, keywords in tech_keywords.items():
            keyword_filters = " OR ".join([f"title LIKE '%{kw}%'" for kw in keywords])
            count = scalar(
                f"SELECT COUNT(*) FROM patents WHERE ({keyword_filters}) AND year >= 2020"
            )
            total = scalar(f"SELECT COUNT(*) FROM patents WHERE year >= 2020")
            pct = (count / total * 100) if total else 0
            tech_recent[tech] = {"count": count, "pct": pct}
        
        return tech_recent
    
    tech_recent = get_emerging_tech()
    
    tech_df = pd.DataFrame(tech_recent).T.reset_index()
    tech_df.columns = ["Technology", "Recent Patents", "Percentage"]
    tech_df = tech_df.sort_values("Recent Patents", ascending=True)
    
    fig, ax = plt.subplots(figsize=(10.2, 5.2))
    colors_tech = [CHART_PALETTE[i % len(CHART_PALETTE)] for i in range(len(tech_df))]
    bars = ax.barh(tech_df["Technology"], tech_df["Recent Patents"], color=colors_tech, edgecolor="#0b1220", linewidth=0.8)
    apply_chart_style(ax, "Emerging Tech Mentions (2020–2024)", "Recent patent titles containing technology keywords")
    
    for bar, val in zip(bars, tech_df["Recent Patents"]):
        ax.text(
            bar.get_width() + 20,
            bar.get_y() + bar.get_height() / 2,
            f"{int(val):,}",
            va="center",
            fontsize=9,
            color=CHART_FG,
        )
    
    plt.tight_layout()
    st.pyplot(fig, clear_figure=True)
    
    st.caption("🔍 Keywords matched in patent titles (case-insensitive). Counts represent patent documents, not unique inventions.")

    st.divider()

    # Hidden Insights (visualized)
    st.markdown("<h3 style='margin-top: 1.2rem; color: #f8fafc;'>Hidden Insights: Inventor & Company Patterns (Visual)</h3>", unsafe_allow_html=True)

    insight_col1, insight_col2 = st.columns(2)

    # Inventor longevity visuals
    with insight_col1:
        st.markdown("<strong style='color: #86efac;'>Inventor Longevity — Distribution & Productivity</strong>", unsafe_allow_html=True)

        longevity = query("""
            SELECT 
                i.inventor_id,
                MIN(p.year) AS first_year,
                MAX(p.year) AS last_year,
                MAX(p.year) - MIN(p.year) + 1 AS career_span,
                COUNT(DISTINCT p.patent_id) AS total_patents
            FROM relationships r
            JOIN inventors i ON r.inventor_id = i.inventor_id
            JOIN patents p ON r.patent_id = p.patent_id
            WHERE p.year IS NOT NULL
            GROUP BY i.inventor_id
            HAVING COUNT(DISTINCT p.patent_id) >= 3
        """)

        if not longevity.empty:
            # Histogram of career spans
            fig, ax = plt.subplots(figsize=(8.6, 3.6))
            spans = longevity['career_span'].dropna().astype(int)
            bins = min(30, spans.max() if len(spans) else 10)
            ax.hist(spans, bins=bins, color=CHART_PALETTE[1], edgecolor='#0b1220', alpha=0.9)
            apply_chart_style(ax, "Inventor Career Span Distribution", "Distribution of inventor active years")
            st.pyplot(fig, clear_figure=True)

            # Scatter: career span vs total patents
            fig2, ax2 = plt.subplots(figsize=(8.6, 3.6))
            sc = ax2.scatter(longevity['career_span'], longevity['total_patents'], s=18, c=longevity['total_patents'], cmap='viridis', alpha=0.6)
            apply_chart_style(ax2, "Patents vs Career Span", "Longer careers generally correlate with higher patent counts")
            ax2.set_xlabel("Career span (years)", color=CHART_MUTED)
            ax2.set_ylabel("Total patents", color=CHART_MUTED)
            cbar = fig2.colorbar(sc, ax=ax2, fraction=0.04, pad=0.01)
            plt.setp(plt.getp(cbar.ax.axes, 'yticklabels'), color=CHART_MUTED)
            st.pyplot(fig2, clear_figure=True)
        else:
            st.info("No inventor longevity data available to visualize.")

    # Company velocity visuals
    with insight_col2:
        st.markdown("<strong style='color: #bfdbfe;'>Company Patent Velocity — Top Firms Comparison</strong>", unsafe_allow_html=True)

        comp_trend = query("""
            SELECT
                c.name,
                SUM(CASE WHEN p.year >= 2015 AND p.year < 2020 THEN 1 ELSE 0 END) AS filings_2015_2019,
                SUM(CASE WHEN p.year >= 2020 AND p.year <= 2024 THEN 1 ELSE 0 END) AS filings_2020_2024
            FROM relationships r
            JOIN companies c ON r.company_id = c.company_id
            JOIN patents p ON r.patent_id = p.patent_id
            GROUP BY c.company_id
            HAVING (filings_2015_2019 + filings_2020_2024) >= 5
            ORDER BY filings_2020_2024 DESC
            LIMIT 12
        """)

        if not comp_trend.empty:
            fig, ax = plt.subplots(figsize=(9.6, 4.8))
            x = np.arange(len(comp_trend))
            width = 0.38
            ax.bar(x - width/2, comp_trend['filings_2015_2019'], width, label='2015–2019', color=CHART_PALETTE[0], alpha=0.9)
            ax.bar(x + width/2, comp_trend['filings_2020_2024'], width, label='2020–2024', color=CHART_PALETTE[5], alpha=0.95)

            ax.set_xticks(x)
            ax.set_xticklabels([n if len(n) <= 18 else n[:15] + '...' for n in comp_trend['name']], rotation=45, ha='right', fontsize=9, color=CHART_MUTED)
            ax.tick_params(colors=CHART_MUTED)
            ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{int(v):,}"))
            ax.legend(frameon=False, labelcolor=CHART_FG)
            apply_chart_style(ax, "Company Filing Velocity: Recent vs Historical", "Grouped comparison of filing counts")
            plt.tight_layout()
            st.pyplot(fig, clear_figure=True)

            st.caption("Bars compare filings in 2015–2019 vs 2020–2024 for top firms; increases indicate acceleration.")
        else:
            st.info("Not enough company filing data to plot velocity comparison.")

    st.divider()
    
    # Productivity Forecast by Inventor Segment
    st.markdown("<h3 style='margin-top: 1.2rem; color: #f8fafc;'>Inventor Productivity Forecast</h3>", unsafe_allow_html=True)
    
    inv_by_year = query("""
        SELECT p.year, COUNT(DISTINCT r.inventor_id) AS inventor_count
        FROM relationships r
        JOIN patents p ON r.patent_id = p.patent_id
        WHERE p.year BETWEEN 2010 AND 2024
        GROUP BY p.year
        ORDER BY p.year
    """)
    
    if not inv_by_year.empty:
        X_inv = inv_by_year["year"].values
        y_inv = inv_by_year["inventor_count"].values
        
        # Simple linear regression
        slope, intercept, r_value, _, _ = stats.linregress(X_inv, y_inv)
        line_pred = slope * X_inv + intercept
        
        # Forecast
        future_inv_years = np.array([2025, 2026, 2027, 2028, 2029])
        future_inv_pred = slope * future_inv_years + intercept
        future_inv_pred = np.maximum(future_inv_pred, y_inv[-1] * 0.95)  # Floor at 95% of latest
        
        fig, ax = plt.subplots(figsize=(11.5, 5.2))
        
        ax.plot(X_inv, y_inv, color="#60a5fa", linewidth=2.6, marker="o", label="Active inventors per year", markersize=5)
        ax.plot(X_inv, line_pred, color="#f472b6", linewidth=2, linestyle="--", label="Linear trend")
        ax.plot(future_inv_years, future_inv_pred, color="#f59e0b", linewidth=2.4, marker="s", label="2025–2029 projection", markersize=5)
        
        ax.set_facecolor("#111827")
        fig.set_facecolor("#111827")
        ax.set_title("Estimated Active Inventors Over Time", fontsize=15, pad=20, color=CHART_FG, fontweight="bold")
        ax.set_xlabel("Year", color=CHART_MUTED)
        ax.set_ylabel("Number of unique inventors", color=CHART_MUTED)
        ax.tick_params(colors=CHART_MUTED, labelsize=9)
        for side in ["top", "right"]:
            ax.spines[side].set_visible(False)
        for side in ["left", "bottom"]:
            ax.spines[side].set_color("#334155")
        ax.grid(True, alpha=0.22, color="#334155")
        ax.set_axisbelow(True)
        ax.legend(frameon=False, labelcolor=CHART_FG, loc="best")
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
        
        plt.tight_layout()
        st.pyplot(fig, clear_figure=True)
        
        recent_inv = y_inv[-1]
        avg_inv = y_inv.mean()
        
        inv_col1, inv_col2, inv_col3 = st.columns(3)
        with inv_col1:
            st.metric("Latest (2024)", f"{int(recent_inv):,} inventors")
        with inv_col2:
            st.metric("Avg. 2010–2024", f"{int(avg_inv):,}")
        with inv_col3:
            st.metric("Trend slope (R²)", f"{r_value**2:.3f}")
        
        st.caption("📊 R² measures goodness-of-fit; higher values indicate stronger linear correlation.")
    else:
        st.info("No inventor activity data available for the forecast period.")

    st.divider()

    # Data Diagnostics Section
    st.markdown("<h3 style='margin-top: 1.2rem; color: #f8fafc;'>📊 Data Quality Diagnostics</h3>", unsafe_allow_html=True)

    diag_col1, diag_col2, diag_col3 = st.columns(3)

    # Missing data analysis
    missing_info = query("""
        SELECT 
            COUNT(CASE WHEN title IS NULL OR title = '' THEN 1 END) AS missing_titles,
            COUNT(CASE WHEN year IS NULL OR year = 0 THEN 1 END) AS missing_years,
            COUNT(CASE WHEN abstract IS NULL OR abstract = '' THEN 1 END) AS missing_abstracts
        FROM patents
    """).iloc[0]

    with diag_col1:
        st.markdown("<strong style='color: #e2e8f0;'>Missing Data</strong>", unsafe_allow_html=True)
        total_p = total_patents
        missing_pct = (missing_info['missing_titles'] / total_p * 100) if total_p else 0
        st.metric("Titles missing", f"{missing_info['missing_titles']:,} ({missing_pct:.2f}%)")
        missing_yr = (missing_info['missing_years'] / total_p * 100) if total_p else 0
        st.metric("Years missing", f"{missing_info['missing_years']:,} ({missing_yr:.2f}%)")
        missing_abs = (missing_info['missing_abstracts'] / total_p * 100) if total_p else 0
        st.metric("Abstracts missing", f"{missing_info['missing_abstracts']:,} ({missing_abs:.2f}%)")

    # Data coverage over time
    with diag_col2:
        st.markdown("<strong style='color: #e2e8f0;'>Coverage Breadth</strong>", unsafe_allow_html=True)
        coverage = query("""
            SELECT 
                COUNT(DISTINCT country) AS unique_countries,
                COUNT(DISTINCT SUBSTR(patent_type, 1, 1)) AS patent_type_variants,
                COUNT(DISTINCT SUBSTR(wipo_kind, 1, 1)) AS wipo_variants
            FROM patents p
            LEFT JOIN relationships r ON p.patent_id = r.patent_id
            LEFT JOIN inventors i ON r.inventor_id = i.inventor_id
        """).iloc[0]

        st.metric("Countries represented", f"{coverage['unique_countries']:,}")
        st.metric("Patent type classes", f"{coverage['patent_type_variants']:,}")
        st.metric("WIPO kind variants", f"{coverage['wipo_variants']:,}")

    # Data recency and outliers
    with diag_col3:
        st.markdown("<strong style='color: #e2e8f0;'>Recency & Outliers</strong>", unsafe_allow_html=True)
        recent_5y = scalar("SELECT COUNT(*) FROM patents WHERE year >= 2020")
        pct_recent = (recent_5y / total_p * 100) if total_p else 0
        st.metric("Patents (2020–2024)", f"{recent_5y:,} ({pct_recent:.1f}%)")

        outlier_info = query("""
            SELECT 
                COUNT(DISTINCT inventor_id) AS mega_inventors
            FROM relationships
            GROUP BY inventor_id
            HAVING COUNT(DISTINCT patent_id) > 100
        """)
        mega_count = len(outlier_info)
        st.metric("'Mega' inventors (100+)", f"{mega_count:,}")
        st.caption("⚠️ High-volume entities may skew analysis.")

    st.divider()

    # Company Velocity Prediction Model (Trained Supervised Regressor)
    st.markdown("<h3 style='margin-top: 1.2rem; color: #f8fafc;'>🚀 Company Patent Velocity Prediction Model</h3>", unsafe_allow_html=True)
    st.markdown("**Trained RandomForest Regressor**: Predicts next-year filings from lagged filing counts, inventor history, and growth indicators.", unsafe_allow_html=True)

    @st.cache_resource
    def load_trained_model():
        model_path = os.path.join(os.path.dirname(DB_PATH), "models", "company_velocity_model.joblib")
        meta_path = os.path.join(os.path.dirname(DB_PATH), "models", "company_velocity_metadata.json")
        try:
            model = joblib.load(model_path)
            with open(meta_path, 'r') as f:
                meta = json.load(f)
            return model, meta
        except Exception as e:
            return None, {"error": str(e)}

    model, model_meta = load_trained_model()

    if model is not None:
        # Display model evaluation metrics
        st.markdown("**Model Evaluation (Test Set):**", unsafe_allow_html=True)
        eval_col1, eval_col2, eval_col3 = st.columns(3)
        rf_metrics = model_meta.get("metrics", {}).get("rf", {})
        with eval_col1:
            st.metric("RMSE", f"{rf_metrics.get('rmse', 0):.4f}")
        with eval_col2:
            st.metric("MAE", f"{rf_metrics.get('mae', 0):.4f}")
        with eval_col3:
            st.metric("R²", f"{rf_metrics.get('r2', 0):.4f}")

        # Get top companies and extract features for prediction
        comp_data = query("""
            SELECT 
                c.company_id,
                c.name,
                c.country,
                COUNT(CASE WHEN p.year >= 2015 AND p.year < 2020 THEN 1 END) AS filings_2015_2019,
                COUNT(CASE WHEN p.year >= 2020 AND p.year <= 2024 THEN 1 END) AS filings_2020_2024,
                COUNT(CASE WHEN p.year = 2023 THEN 1 ELSE 0 END) AS filings_2023,
                COUNT(CASE WHEN p.year = 2022 THEN 1 ELSE 0 END) AS filings_2022,
                COUNT(DISTINCT r.inventor_id) AS total_inventors
            FROM relationships r
            JOIN companies c ON r.company_id = c.company_id
            JOIN patents p ON r.patent_id = p.patent_id
            GROUP BY c.company_id
            HAVING COUNT(DISTINCT p.patent_id) >= 10
            ORDER BY filings_2020_2024 DESC
            LIMIT 12
        """)

        if not comp_data.empty:
            # Extract inventor counts per company per year
            inv_data = query("""
                SELECT c.company_id, p.year, COUNT(DISTINCT r.inventor_id) AS inventors
                FROM relationships r
                JOIN companies c ON r.company_id = c.company_id
                JOIN patents p ON r.patent_id = p.patent_id
                WHERE p.year BETWEEN 2022 AND 2024
                GROUP BY c.company_id, p.year
            """)
            inv_pivot = inv_data.pivot_table(index="company_id", columns="year", values="inventors", fill_value=0)

            # Prepare features for model prediction
            features = model_meta.get("features", [])
            le_classes = model_meta.get("label_encoder_classes", [])
            le = LabelEncoder()
            le.classes_ = np.array(le_classes)

            preds = []
            for idx, row in comp_data.iterrows():
                company_id = row["company_id"]
                try:
                    inv_2023 = int(inv_pivot.get(2023, {}).get(company_id, 0) or 0)
                    inv_2022 = int(inv_pivot.get(2022, {}).get(company_id, 0) or 0)
                    inv_2021 = int(inv_pivot.get(2021, {}).get(company_id, 0) or 0)
                except:
                    inv_2023 = inv_2022 = inv_2021 = 0

                filings_current = int(row["filings_2023"] or 0)
                filings_lag1 = int(row["filings_2022"] or 0)
                filings_lag2 = int(row["filings_2015_2019"] or 0)
                filings_growth = (filings_current - filings_lag1) / (filings_lag1 + 1)
                inventor_growth = (inv_2023 - inv_2022) / (inv_2022 + 1) if inv_2022 > 0 else 0
                country_code = le.transform([row["country"]])[0]
                year = 2024

                X_sample = np.array([[filings_current, filings_lag1, filings_lag2, inv_2023, inv_2022, inv_2021, filings_growth, inventor_growth, country_code, year]])
                pred_2025 = max(0, int(model.predict(X_sample)[0]))

                # Annualize the 2020–2024 window for fair comparison with single-year prediction
                avg_2020_2024 = int(row["filings_2020_2024"] / 5) if row.get("filings_2020_2024") is not None else 0

                preds.append({
                    "Company": row["name"][:20],
                    "Country": row["country"],
                    "2020–2024 (Annual avg)": avg_2020_2024,
                    "Predicted 2025 Filings": pred_2025,
                    "Change": pred_2025 - avg_2020_2024,
                })

            pred_df = pd.DataFrame(preds)

            # Visualization: predicted vs historical (annualized)
            fig, ax = plt.subplots(figsize=(12, 5.8))
            x_pos = np.arange(len(pred_df))
            width = 0.35

            bars1 = ax.bar(x_pos - width/2, pred_df["2020–2024 (Annual avg)"], width, label='2020–2024 (Annual avg)', color='#38bdf8', alpha=0.8)
            bars2 = ax.bar(x_pos + width/2, pred_df["Predicted 2025 Filings"], width, label='2025 (Predicted)', color='#f472b6', alpha=0.8)

            ax.set_facecolor("#0b1220")
            fig.set_facecolor("#0b1220")
            apply_chart_style(ax, "Company Patent Filings: annualized 2020–2024 vs 2025 Forecast (ML Model)", "Annualized actuals (avg) shown for 2020–2024 window")
            ax.set_xlabel("Company", fontsize=10, color=CHART_MUTED)
            ax.set_ylabel("Patent filings (annual)", fontsize=10, color=CHART_MUTED)
            ax.set_xticks(x_pos)
            ax.set_xticklabels(pred_df["Company"], rotation=45, ha='right', fontsize=8, color=CHART_MUTED)
            ax.tick_params(colors=CHART_MUTED, labelsize=8)
            ax.legend(frameon=False, labelcolor=CHART_FG, loc='upper left')
            ax.grid(True, axis='y', alpha=0.2, color='#334155')
            ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x)}"))

            for side in ["top", "right"]:
                ax.spines[side].set_visible(False)
            for side in ["left", "bottom"]:
                ax.spines[side].set_color("#334155")

            plt.tight_layout()
            st.pyplot(fig, clear_figure=True)

            # Summary metrics
            st.markdown("**2025 Forecast Summary:**", unsafe_allow_html=True)
            summary_col1, summary_col2, summary_col3, summary_col4 = st.columns(4)
            avg_change = pred_df["Change"].mean()
            max_growth = pred_df["Change"].max()
            companies_growing = (pred_df["Change"] > 0).sum()

            with summary_col1:
                st.metric("Avg. predicted change", f"{avg_change:.0f} filings")
            with summary_col2:
                st.metric("Max predicted growth", f"{max_growth} filings")
            with summary_col3:
                st.metric("Companies growing", f"{companies_growing}/{len(pred_df)}")
            with summary_col4:
                st.metric("Avg. 2025 forecast", f"{pred_df['Predicted 2025 Filings'].mean():.0f}")

            st.caption("📊 **Model Details**: Trained RandomForest (n_estimators=300) on company-year panels (2012–2024) using lagged filings, inventor counts, and growth rates. Test R²=0.964. Predictions use 2022–2024 recent history.")
        else:
            st.info("Insufficient company data for model prediction (require 10+ patents).")
    else:
        st.warning("⚠️ Trained model not found. Run `python scripts/train_company_velocity.py` first.")
