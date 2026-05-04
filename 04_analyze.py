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
def load_queries(sql_path: str) -> dict[str, str]:
    """Load named SQL blocks from queries.sql.

    The file uses comment headers like:
    -- Q1_top_inventors
    SELECT ...
    """
    if not os.path.exists(sql_path):
        raise FileNotFoundError(f"Query file not found at {sql_path}")

    queries: dict[str, list[str]] = {}
    current_name: str | None = None

    with open(sql_path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.rstrip("\n")
            stripped = line.strip()

            if stripped.startswith("-- Q"):
                current_name = stripped[3:].strip()
                queries[current_name] = []
                continue

            if current_name is None:
                continue

            if not stripped:
                queries[current_name].append("")
                continue

            if set(stripped) == {"─"}:
                continue

            queries[current_name].append(line)

    return {
        name: "\n".join(lines).strip()
        for name, lines in queries.items()
        if "\n".join(lines).strip()
    }


QUERIES = load_queries(QUERIES_SQL)

EXPECTED_QUERIES = {
    "Q1_top_inventors",
    "Q2_top_companies",
    "Q3_top_countries",
    "Q4_patents_per_year",
    "Q5_join_query",
    "Q6_cte_query",
    "Q7_inventor_ranking",
}

missing_queries = EXPECTED_QUERIES.difference(QUERIES)
if missing_queries:
    raise ValueError(
        f"Missing expected query blocks in {QUERIES_SQL}: {sorted(missing_queries)}"
    )


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

    conn.close()
    print("\nAnalysis complete. Reports saved to:", REPORTS_DIR.resolve())
    
