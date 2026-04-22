"""
Step 4: Run all 7 required SQL analytical queries against the SQLite database.
Saves results to CSV and JSON, and prints a formatted console report.
"""

import os
import json
import sqlite3
import pandas as pd
from project_paths import DB_PATH, QUERIES_SQL, REPORTS_DIR, ensure_directories

# ─── Paths ────────────────────────────────────────────────────────────────────
ensure_directories()


# ─── SQL Queries ─────────────────────────────────────────────────────────────
QUERIES = {

    # Q1 — Top Inventors (most patents)
    "Q1_top_inventors": """
        SELECT
            i.name                              AS inventor,
            i.country,
            COUNT(DISTINCT r.patent_id)         AS patent_count
        FROM relationships r
        JOIN inventors i ON r.inventor_id = i.inventor_id
        GROUP BY r.inventor_id
        ORDER BY patent_count DESC
        LIMIT 20;
    """,

    # Q2 — Top Companies
    "Q2_top_companies": """
        SELECT
            c.name                              AS company,
            c.country,
            COUNT(DISTINCT r.patent_id)         AS patent_count
        FROM relationships r
        JOIN companies c ON r.company_id = c.company_id
        GROUP BY r.company_id
        ORDER BY patent_count DESC
        LIMIT 20;
    """,

    # Q3 — Top Countries (by inventor country)
    "Q3_top_countries": """
        SELECT
            i.country,
            COUNT(DISTINCT r.patent_id)         AS patent_count,
            ROUND(
                COUNT(DISTINCT r.patent_id) * 100.0 /
                (SELECT COUNT(*) FROM patents), 2
            )                                   AS pct_share
        FROM relationships r
        JOIN inventors i ON r.inventor_id = i.inventor_id
        WHERE i.country != 'Unknown' AND i.country != ''
        GROUP BY i.country
        ORDER BY patent_count DESC
        LIMIT 20;
    """,

    # Q4 — Patents per Year (trend over time)
    "Q4_patents_per_year": """
        SELECT
            year,
            COUNT(*)                            AS patent_count
        FROM patents
        WHERE year IS NOT NULL
          AND year BETWEEN 1976 AND 2025
        GROUP BY year
        ORDER BY year;
    """,

    # Q5 — JOIN: Patents with their inventors AND companies
    "Q5_join_query": """
        SELECT
            p.patent_id,
            p.title,
            p.year,
            i.name                              AS inventor_name,
            i.country                           AS inventor_country,
            c.name                              AS company_name
        FROM patents p
        LEFT JOIN relationships r  ON p.patent_id  = r.patent_id
        LEFT JOIN inventors     i  ON r.inventor_id = i.inventor_id
        LEFT JOIN companies     c  ON r.company_id  = c.company_id
        WHERE p.year >= 2000
        LIMIT 500;
    """,

    # Q6 — CTE: Top companies per year (last 10 years)
    "Q6_cte_query": """
        WITH yearly_company_counts AS (
            -- Step 1: count patents per company per year
            SELECT
                p.year,
                c.name                          AS company,
                COUNT(DISTINCT r.patent_id)     AS patent_count
            FROM patents p
            JOIN relationships r ON p.patent_id  = r.patent_id
            JOIN companies     c ON r.company_id = c.company_id
            WHERE p.year BETWEEN 2015 AND 2024
              AND c.name IS NOT NULL
            GROUP BY p.year, c.name
        ),
        ranked AS (
            -- Step 2: rank companies within each year
            SELECT
                year,
                company,
                patent_count,
                RANK() OVER (
                    PARTITION BY year
                    ORDER BY patent_count DESC
                ) AS yearly_rank
            FROM yearly_company_counts
        )
        -- Step 3: keep only top 5 per year
        SELECT year, yearly_rank, company, patent_count
        FROM ranked
        WHERE yearly_rank <= 5
        ORDER BY year DESC, yearly_rank;
    """,

    # Q7 — Window Function: Rank all inventors by patent count
    "Q7_inventor_ranking": """
        SELECT
            RANK() OVER (ORDER BY patent_count DESC)        AS rank,
            DENSE_RANK() OVER (ORDER BY patent_count DESC)  AS dense_rank,
            inventor,
            country,
            patent_count,
            ROUND(
                patent_count * 100.0 /
                SUM(patent_count) OVER (), 4
            )                                               AS pct_of_total
        FROM (
            SELECT
                i.name          AS inventor,
                i.country,
                COUNT(DISTINCT r.patent_id) AS patent_count
            FROM relationships r
            JOIN inventors i ON r.inventor_id = i.inventor_id
            GROUP BY r.inventor_id
        ) sub
        ORDER BY patent_count DESC
        LIMIT 50;
    """,
}


# ─── Run queries ─────────────────────────────────────────────────────────────
def run_all(conn: sqlite3.Connection) -> dict[str, pd.DataFrame]:
    results = {}
    for name, sql in QUERIES.items():
        print(f"  Running {name} … ", end="", flush=True)
        df = pd.read_sql_query(sql, conn)
        results[name] = df
        print(f"{len(df):,} rows")
    return results


# ─── Console Report ───────────────────────────────────────────────────────────
def console_report(conn: sqlite3.Connection, results: dict) -> None:
    total = conn.execute("SELECT COUNT(*) FROM patents").fetchone()[0]
    first = conn.execute("SELECT MIN(year) FROM patents WHERE year IS NOT NULL").fetchone()[0]
    last  = conn.execute("SELECT MAX(year) FROM patents WHERE year IS NOT NULL").fetchone()[0]

    print()
    print("=" * 60)
    print("           GLOBAL PATENT INTELLIGENCE REPORT")
    print("=" * 60)
    print(f"  Total Patents  :  {total:,}")
    print(f"  Year Range     :  {first} – {last}")
    print()

    # Top 10 inventors
    print("── Top 10 Inventors ─────────────────────────────────────────")
    for i, row in results["Q1_top_inventors"].head(10).iterrows():
        print(f"  {i+1:>2}. {row['inventor']:<35} {row['patent_count']:>5} patents  [{row['country']}]")

    print()
    print("── Top 10 Companies ─────────────────────────────────────────")
    for i, row in results["Q2_top_companies"].head(10).iterrows():
        print(f"  {i+1:>2}. {row['company']:<35} {row['patent_count']:>5} patents  [{row['country']}]")

    print()
    print("── Top 10 Countries ─────────────────────────────────────────")
    for i, row in results["Q3_top_countries"].head(10).iterrows():
        print(f"  {i+1:>2}. {row['country']:<10}  {row['patent_count']:>7,} patents  ({row['pct_share']:.2f}%)")

    print()
    print("── Patent Trend (last 10 years) ─────────────────────────────")
    trend = results["Q4_patents_per_year"].tail(10)
    max_count = trend["patent_count"].max()
    for _, row in trend.iterrows():
        bar_len = int(row["patent_count"] / max_count * 30)
        bar = "█" * bar_len
        print(f"  {int(row['year'])}  {bar:<30}  {row['patent_count']:,}")

    print()
    print("=" * 60)


# ─── Export CSV ───────────────────────────────────────────────────────────────
def export_csv(results: dict) -> None:
    exports = {
        "top_inventors":  results["Q1_top_inventors"],
        "top_companies":  results["Q2_top_companies"],
        "country_trends": results["Q3_top_countries"],
        "yearly_trends":  results["Q4_patents_per_year"],
    }
    for name, df in exports.items():
        path = os.path.join(REPORTS_DIR, f"{name}.csv")
        df.to_csv(path, index=False)
        print(f"  CSV  → {path}")


# ─── Export JSON ─────────────────────────────────────────────────────────────
def export_json(conn: sqlite3.Connection, results: dict) -> None:
    total    = conn.execute("SELECT COUNT(*) FROM patents").fetchone()[0]
    inv_top  = results["Q1_top_inventors"].head(10)
    comp_top = results["Q2_top_companies"].head(10)
    cty_top  = results["Q3_top_countries"].head(10)

    report = {
        "total_patents": total,
        "top_inventors": [
            {"rank": i+1, "name": r["inventor"], "country": r["country"],
             "patents": int(r["patent_count"])}
            for i, r in inv_top.iterrows()
        ],
        "top_companies": [
            {"rank": i+1, "name": r["company"], "country": r["country"],
             "patents": int(r["patent_count"])}
            for i, r in comp_top.iterrows()
        ],
        "top_countries": [
            {"country": r["country"], "patents": int(r["patent_count"]),
             "share": float(r["pct_share"])}
            for _, r in cty_top.iterrows()
        ],
        "yearly_trend": [
            {"year": int(r["year"]), "patents": int(r["patent_count"])}
            for _, r in results["Q4_patents_per_year"].iterrows()
        ],
    }

    path = os.path.join(REPORTS_DIR, "patent_report.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(f"  JSON → {path}")


# ─── Save SQL queries to file ─────────────────────────────────────────────────
def export_sql_queries() -> None:
    with open(QUERIES_SQL, "w", encoding="utf-8") as f:
        for name, sql in QUERIES.items():
            f.write(f"-- {name}\n{sql.strip()}\n\n{'─'*60}\n\n")
    print(f"  SQL  → {QUERIES_SQL}")


# ─── Main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("  Patent Analytics Engine")
    print("=" * 60)

    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(
            f"Database not found at {DB_PATH}\n"
            "Please run 03_load_database.py first."
        )

    conn = sqlite3.connect(DB_PATH)

    print("\n── Running queries ───────────────────────────────────────────")
    results = run_all(conn)

    console_report(conn, results)

    print("── Exporting reports ─────────────────────────────────────────")
    export_csv(results)
    export_json(conn, results)
    export_sql_queries()

    conn.close()
    print("\n✅  Analysis complete. Reports saved to:", REPORTS_DIR.resolve())
    print("   Optional next step → run  05_visualize.py  for charts")
