"""
Step 3: Load clean CSV files into a SQLite database.
Creates all tables, indexes, and foreign keys.
"""

import os
import sqlite3
import pandas as pd
from project_paths import CLEAN_DIR, DB_PATH, SCHEMA_SQL, ensure_directories

# ─── Paths ────────────────────────────────────────────────────────────────────
ensure_directories()


# ─── Schema ───────────────────────────────────────────────────────────────────
SCHEMA = """
-- ─── Drop existing tables (clean slate) ─────────────────────────────────────
DROP TABLE IF EXISTS relationships;
DROP TABLE IF EXISTS patents;
DROP TABLE IF EXISTS inventors;
DROP TABLE IF EXISTS companies;

-- ─── patents ─────────────────────────────────────────────────────────────────
CREATE TABLE patents (
    patent_id   TEXT    PRIMARY KEY,
    title       TEXT    NOT NULL,
    abstract    TEXT,
    filing_date TEXT,
    year        INTEGER,
    patent_type TEXT,
    wipo_kind   TEXT
);

-- ─── inventors ───────────────────────────────────────────────────────────────
CREATE TABLE inventors (
    inventor_id TEXT    PRIMARY KEY,
    name        TEXT    NOT NULL,
    country     TEXT
);

-- ─── companies ───────────────────────────────────────────────────────────────
CREATE TABLE companies (
    company_id  TEXT    PRIMARY KEY,
    name        TEXT    NOT NULL,
    country     TEXT
);

-- ─── relationships ───────────────────────────────────────────────────────────
CREATE TABLE relationships (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    patent_id   TEXT,
    inventor_id TEXT,
    company_id  TEXT,
    FOREIGN KEY (patent_id)   REFERENCES patents(patent_id),
    FOREIGN KEY (inventor_id) REFERENCES inventors(inventor_id),
    FOREIGN KEY (company_id)  REFERENCES companies(company_id)
);

-- ─── Indexes (speed up queries) ──────────────────────────────────────────────
CREATE INDEX idx_patents_year         ON patents(year);
CREATE INDEX idx_rel_patent_id        ON relationships(patent_id);
CREATE INDEX idx_rel_inventor_id      ON relationships(inventor_id);
CREATE INDEX idx_rel_company_id       ON relationships(company_id);
"""


# ─── Helpers ──────────────────────────────────────────────────────────────────
def load_csv(conn: sqlite3.Connection, filename: str, table: str) -> None:
    path = CLEAN_DIR / filename
    if not os.path.exists(path):
        print(f"  [WARN] {filename} not found — skipping {table}")
        return

    df = pd.read_csv(path, low_memory=False)

    # Drop duplicate-name artifacts like "inventor_id.1" when base column exists.
    drop_cols = []
    for col in df.columns:
        if "." in col:
            base = col.split(".", 1)[0]
            if base in df.columns:
                drop_cols.append(col)
    if drop_cols:
        df = df.drop(columns=drop_cols)

    # Align to actual DB schema columns for this table.
    table_cols = [
        row[1]
        for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
    ]
    if "id" in table_cols and table == "relationships":
        table_cols.remove("id")

    missing = [c for c in table_cols if c not in df.columns]
    if missing:
        print(f"  [WARN] {table}: missing CSV columns {missing}; filling with NULL")
        for col in missing:
            df[col] = None

    extra = [c for c in df.columns if c not in table_cols]
    if extra:
        df = df.drop(columns=extra)

    df = df[table_cols]

    if table == "relationships":
        # Keep only relationships for loaded patents and null-out orphan foreign keys.
        valid_patents = set(
            pd.read_sql_query("SELECT patent_id FROM patents", conn)["patent_id"].astype(str)
        )
        valid_inventors = set(
            pd.read_sql_query("SELECT inventor_id FROM inventors", conn)["inventor_id"].astype(str)
        )
        valid_companies = set(
            pd.read_sql_query("SELECT company_id FROM companies", conn)["company_id"].astype(str)
        )

        df["patent_id"] = df["patent_id"].astype(str)
        df = df[df["patent_id"].isin(valid_patents)]

        if "inventor_id" in df.columns:
            df["inventor_id"] = df["inventor_id"].astype(str)
            df["inventor_id"] = df["inventor_id"].where(
                df["inventor_id"].isin(valid_inventors),
                None,
            )

        if "company_id" in df.columns:
            df["company_id"] = df["company_id"].astype(str)
            df["company_id"] = df["company_id"].where(
                df["company_id"].isin(valid_companies),
                None,
            )

    print(f"  Loading {table:15s}  ({len(df):>10,} rows) … ", end="", flush=True)
    df.to_sql(table, conn, if_exists="append", index=False, chunksize=50_000)
    print("done")


# ─── Main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("  PatentsView → SQLite Loader")
    print("=" * 60)

    # Write schema.sql for submission
    SCHEMA_SQL.parent.mkdir(parents=True, exist_ok=True)
    with open(SCHEMA_SQL, "w", encoding="utf-8") as f:
        f.write(SCHEMA)
    print(f"\n  schema.sql written → {SCHEMA_SQL}\n")

    # Connect (creates the file if it doesn't exist)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")

    # Apply schema
    conn.executescript(SCHEMA)
    conn.commit()
    print("  Schema applied.\n")

    # Load tables in dependency order
    load_csv(conn, "clean_patents.csv",       "patents")
    load_csv(conn, "clean_inventors.csv",     "inventors")
    load_csv(conn, "clean_companies.csv",     "companies")
    load_csv(conn, "clean_relationships.csv", "relationships")

    conn.commit()

    # Quick row-count verification
    print("\n── Row counts ───────────────────────────────────────────────")
    for table in ["patents", "inventors", "companies", "relationships"]:
        n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table:20s}  {n:>12,}")

    conn.close()
    print(f"\nDatabase saved to: {DB_PATH.resolve()}")

