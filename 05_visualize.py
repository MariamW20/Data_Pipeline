"""
Step 5 (Bonus): Generate charts from the analysis results.
Saves PNG charts to the reports/ directory.
"""

import os
import json
import sqlite3
import pandas as pd
import matplotlib
matplotlib.use("Agg")            # non-interactive backend (works without display)
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from project_paths import DB_PATH, REPORTS_DIR, ensure_directories

# ─── Paths ────────────────────────────────────────────────────────────────────
ensure_directories()

# Style
plt.rcParams.update({
    "figure.facecolor": "#0f172a",
    "axes.facecolor":   "#1e293b",
    "axes.edgecolor":   "#334155",
    "axes.labelcolor":  "#cbd5e1",
    "xtick.color":      "#94a3b8",
    "ytick.color":      "#94a3b8",
    "text.color":       "#f1f5f9",
    "grid.color":       "#334155",
    "grid.linestyle":   "--",
    "grid.alpha":       0.5,
    "font.family":      "DejaVu Sans",
})

ACCENT = "#38bdf8"   # sky-400
ACCENT2 = "#f472b6"  # pink-400
GRADIENT = ["#0ea5e9", "#38bdf8", "#7dd3fc", "#bae6fd",
            "#e0f2fe", "#f0f9ff", "#f8fafc", "#cbd5e1",
            "#94a3b8", "#64748b"]


# ─── Load data ────────────────────────────────────────────────────────────────
def load(conn: sqlite3.Connection, sql: str) -> pd.DataFrame:
    return pd.read_sql_query(sql, conn)


# ─── Chart 1: Patents per Year ────────────────────────────────────────────────
def chart_yearly_trend(conn: sqlite3.Connection) -> None:
    df = load(conn, """
        SELECT year, COUNT(*) AS cnt
        FROM patents
        WHERE year BETWEEN 1980 AND 2024
        GROUP BY year ORDER BY year
    """)

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.fill_between(df["year"], df["cnt"], alpha=0.25, color=ACCENT)
    ax.plot(df["year"], df["cnt"], color=ACCENT, linewidth=2)
    ax.set_title("Patents Granted per Year", fontsize=16, pad=14, color="#f1f5f9")
    ax.set_xlabel("Year")
    ax.set_ylabel("Number of Patents")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.grid(True, axis="y")
    plt.tight_layout()
    path = os.path.join(REPORTS_DIR, "chart_yearly_trend.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved → {path}")


# ─── Chart 2: Top 15 Companies ───────────────────────────────────────────────
def chart_top_companies(conn: sqlite3.Connection) -> None:
    df = load(conn, """
        SELECT c.name AS company, COUNT(DISTINCT r.patent_id) AS cnt
        FROM relationships r
        JOIN companies c ON r.company_id = c.company_id
        GROUP BY r.company_id
        ORDER BY cnt DESC LIMIT 15
    """)
    df = df.sort_values("cnt")

    fig, ax = plt.subplots(figsize=(10, 7))
    colors = [GRADIENT[i % len(GRADIENT)] for i in range(len(df))]
    bars = ax.barh(df["company"], df["cnt"], color=colors, height=0.65)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.set_title("Top 15 Companies by Patent Count", fontsize=15, pad=12)
    ax.set_xlabel("Number of Patents")
    for bar, val in zip(bars, df["cnt"]):
        ax.text(bar.get_width() + bar.get_width() * 0.01, bar.get_y() + bar.get_height() / 2,
                f"{val:,}", va="center", fontsize=8, color="#cbd5e1")
    ax.grid(True, axis="x")
    plt.tight_layout()
    path = os.path.join(REPORTS_DIR, "chart_top_companies.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved → {path}")


# ─── Chart 3: Top Countries ───────────────────────────────────────────────────
def chart_top_countries(conn: sqlite3.Connection) -> None:
    df = load(conn, """
        SELECT i.country, COUNT(DISTINCT r.patent_id) AS cnt
        FROM relationships r
        JOIN inventors i ON r.inventor_id = i.inventor_id
        WHERE i.country NOT IN ('Unknown', '', 'XX')
        GROUP BY i.country
        ORDER BY cnt DESC LIMIT 12
    """)

    fig, ax = plt.subplots(figsize=(8, 8))
    wedges, texts, autotexts = ax.pie(
        df["cnt"],
        labels=df["country"],
        autopct="%1.1f%%",
        colors=GRADIENT,
        startangle=140,
        pctdistance=0.82,
    )
    for t in autotexts:
        t.set_fontsize(8)
        t.set_color("#0f172a")
    ax.set_title("Patent Share by Country", fontsize=15, pad=16)
    plt.tight_layout()
    path = os.path.join(REPORTS_DIR, "chart_country_share.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved → {path}")


# ─── Chart 4: Top 15 Inventors ───────────────────────────────────────────────
def chart_top_inventors(conn: sqlite3.Connection) -> None:
    df = load(conn, """
        SELECT i.name AS inventor, i.country,
               COUNT(DISTINCT r.patent_id) AS cnt
        FROM relationships r
        JOIN inventors i ON r.inventor_id = i.inventor_id
        GROUP BY r.inventor_id
        ORDER BY cnt DESC LIMIT 15
    """)
    df = df.sort_values("cnt")
    label = df["inventor"] + "  [" + df["country"] + "]"

    fig, ax = plt.subplots(figsize=(10, 7))
    bars = ax.barh(label, df["cnt"], color=ACCENT2, height=0.65)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.set_title("Top 15 Inventors by Patent Count", fontsize=15, pad=12)
    ax.set_xlabel("Number of Patents")
    for bar, val in zip(bars, df["cnt"]):
        ax.text(bar.get_width() + bar.get_width() * 0.01, bar.get_y() + bar.get_height() / 2,
                f"{val:,}", va="center", fontsize=8, color="#cbd5e1")
    ax.grid(True, axis="x")
    plt.tight_layout()
    path = os.path.join(REPORTS_DIR, "chart_top_inventors.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved → {path}")


# ─── Main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("  Patent Visualizer")
    print("=" * 60)

    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(
            f"Database not found: {DB_PATH}\n"
            "Run 03_load_database.py first."
        )

    conn = sqlite3.connect(DB_PATH)

    print("\n── Generating charts ─────────────────────────────────────────")
    chart_yearly_trend(conn)
    chart_top_companies(conn)
    chart_top_countries(conn)
    chart_top_inventors(conn)

    conn.close()
    print(f"\n✅  Charts saved to: {REPORTS_DIR.resolve()}")
    print("   Optional next step → run  06_dashboard.py  for Streamlit")
