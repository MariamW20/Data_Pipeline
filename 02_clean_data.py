"""
Step 2: Clean raw PatentsView TSV files with pandas.
Produces clean CSVs ready for loading into SQLite.
"""

import os
import sqlite3
import csv
import pandas as pd
from project_paths import CLEAN_DIR, RAW_DIR, ensure_directories

try:
    import pycountry
except Exception:
    pycountry = None

# ─── Paths ────────────────────────────────────────────────────────────────────
ensure_directories()

# Patent row limit configuration.
# Default is full load. To sample, set PATENT_SAMPLE_ROWS env var, e.g. 200000.
_sample_rows_env = os.getenv("PATENT_SAMPLE_ROWS", "").strip()
SAMPLE_ROWS = int(_sample_rows_env) if _sample_rows_env else None


# ─── Helpers ──────────────────────────────────────────────────────────────────
def read_tsv(filename: str, usecols: list, nrows=None) -> pd.DataFrame:
    """Read a TSV from the raw directory, keeping only available columns."""
    path = RAW_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Missing raw file: {path}")

    header_cols = pd.read_csv(path, sep="\t", nrows=0).columns.tolist()
    selected_cols = [c for c in usecols if c in header_cols]
    missing_cols = [c for c in usecols if c not in header_cols]

    if not selected_cols:
        raise ValueError(
            f"None of the requested columns exist in {filename}. "
            f"Requested: {usecols} | Available: {header_cols}"
        )

    print(f"  Reading  {filename} …", end="", flush=True)
    df = pd.read_csv(
        path,
        sep="\t",
        usecols=selected_cols,
        nrows=nrows,
        low_memory=False,
        on_bad_lines="skip",
    )
    print(f"  {len(df):,} rows")
    if missing_cols:
        print(f"    [INFO] Missing columns in {filename}: {missing_cols}")
    return df


def iter_tsv_chunks(filename: str, usecols: list, chunksize: int = 500_000, nrows=None):
    """Yield TSV chunks from a raw file using the columns that actually exist."""
    path = RAW_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Missing raw file: {path}")

    header_cols = pd.read_csv(path, sep="\t", nrows=0).columns.tolist()
    selected_cols = [c for c in usecols if c in header_cols]
    missing_cols = [c for c in usecols if c not in header_cols]

    if not selected_cols:
        raise ValueError(
            f"None of the requested columns exist in {filename}. "
            f"Requested: {usecols} | Available: {header_cols}"
        )

    print(f"  Streaming {filename} …", flush=True)
    if missing_cols:
        print(f"    [INFO] Missing columns in {filename}: {missing_cols}")

    return pd.read_csv(
        path,
        sep="\t",
        usecols=selected_cols,
        nrows=nrows,
        chunksize=chunksize,
        engine="python",
        on_bad_lines="skip",
    )


def save_clean(df: pd.DataFrame, name: str) -> None:
    path = CLEAN_DIR / f"clean_{name}.csv"
    df.to_csv(path, index=False)
    print(f"  ✓ Saved  {path}  ({len(df):,} rows)\n")


def prepare_abstract_lookup_db() -> sqlite3.Connection | None:
    """Build a temporary on-disk lookup table for patent abstracts."""
    abstract_file = RAW_DIR / "g_patent_abstract.tsv"
    if not abstract_file.exists():
        print("    [INFO] g_patent_abstract.tsv not found; abstracts will be empty")
        return None

    tmp_db = CLEAN_DIR / "_abstract_lookup.sqlite"
    if tmp_db.exists():
        tmp_db.unlink()

    conn = sqlite3.connect(tmp_db)
    conn.execute("CREATE TABLE abstracts (patent_id TEXT, patent_abstract TEXT)")

    print("  Streaming g_patent_abstract.tsv …", flush=True)
    with open(abstract_file, "r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.reader(f, delimiter="\t")
        header = next(reader, None)
        if not header:
            conn.execute("CREATE INDEX idx_abstracts_patent_id ON abstracts(patent_id)")
            conn.commit()
            return conn

        header = [h.strip().strip('"') for h in header]
        id_idx = header.index("patent_id") if "patent_id" in header else None
        abs_idx = header.index("patent_abstract") if "patent_abstract" in header else None
        if id_idx is None or abs_idx is None:
            conn.execute("CREATE INDEX idx_abstracts_patent_id ON abstracts(patent_id)")
            conn.commit()
            return conn

        batch = []
        for row in reader:
            if not row:
                continue
            if id_idx >= len(row):
                continue
            patent_id = (row[id_idx] or "").strip().strip('"')
            if not patent_id:
                continue

            patent_abstract = ""
            if abs_idx < len(row):
                patent_abstract = (row[abs_idx] or "").strip().strip('"')

            batch.append((patent_id, patent_abstract))
            if len(batch) >= 50_000:
                conn.executemany("INSERT INTO abstracts(patent_id, patent_abstract) VALUES (?, ?)", batch)
                conn.commit()
                batch.clear()

        if batch:
            conn.executemany("INSERT INTO abstracts(patent_id, patent_abstract) VALUES (?, ?)", batch)
            conn.commit()

    conn.execute("CREATE INDEX idx_abstracts_patent_id ON abstracts(patent_id)")
    conn.commit()
    return conn


COUNTRY_FALLBACK = {
    "US": "United States",
    "JP": "Japan",
    "DE": "Germany",
    "CN": "China",
    "KR": "South Korea",
    "FR": "France",
    "CA": "Canada",
    "GB": "United Kingdom",
    "TW": "Taiwan",
    "IN": "India",
    "IT": "Italy",
    "NL": "Netherlands",
    "SE": "Sweden",
    "CH": "Switzerland",
    "AU": "Australia",
}


def country_code_to_name(value: str) -> str:
    code = (value or "").strip().upper()
    if not code or code == "UNKNOWN":
        return "Unknown"

    if len(code) > 2:
        return (value or "").strip().title()

    if pycountry is not None and len(code) == 2 and code.isalpha():
        match = pycountry.countries.get(alpha_2=code)
        if match is not None:
            return match.name

    return COUNTRY_FALLBACK.get(code, code)


def normalize_country_series(series: pd.Series) -> pd.Series:
    return series.fillna("Unknown").astype(str).str.upper().str.strip().map(country_code_to_name)


def load_location_country_map() -> pd.DataFrame:
    """Load location_id -> country mapping if location data is available."""
    location_file = RAW_DIR / "g_location_disambiguated.tsv"
    if not location_file.exists():
        print("  [INFO] g_location_disambiguated.tsv not found; countries may be Unknown")
        return pd.DataFrame(columns=["location_id", "country_from_location"])

    loc = read_tsv(
        "g_location_disambiguated.tsv",
        usecols=["location_id", "disambig_country"],
    )
    loc = loc.rename(columns={"disambig_country": "country_from_location"})
    loc["country_from_location"] = normalize_country_series(loc["country_from_location"])
    loc = loc.dropna(subset=["location_id"]).drop_duplicates(subset=["location_id"])
    return loc


# ─── 1. Patents ───────────────────────────────────────────────────────────────
def clean_patents() -> pd.DataFrame:
    print("\n── Patents ──────────────────────────────────────────────────")

    # Full-load mode: stream g_patent.tsv in chunks and use an on-disk abstract lookup.
    if SAMPLE_ROWS is None:
        lookup_conn = prepare_abstract_lookup_db()
        out_path = CLEAN_DIR / "clean_patents.csv"
        if out_path.exists():
            out_path.unlink()

        total_saved = 0
        total_dropped = 0
        first_write = True

        for chunk in iter_tsv_chunks(
            "g_patent.tsv",
            usecols=["patent_id", "patent_title", "patent_date", "patent_type", "wipo_kind"],
            chunksize=300_000,
        ):
            chunk["patent_id"] = chunk["patent_id"].astype(str).str.strip()
            if "patent_type" not in chunk.columns:
                chunk["patent_type"] = ""
            if "wipo_kind" not in chunk.columns:
                chunk["wipo_kind"] = ""

            if lookup_conn is not None:
                ids = chunk[["patent_id"]].drop_duplicates()
                ids.to_sql("chunk_ids", lookup_conn, if_exists="replace", index=False)
                abs_df = pd.read_sql_query(
                    """
                    SELECT a.patent_id, a.patent_abstract
                    FROM abstracts a
                    INNER JOIN chunk_ids c ON a.patent_id = c.patent_id
                    """,
                    lookup_conn,
                )
                abstract_map = dict(zip(abs_df["patent_id"], abs_df["patent_abstract"]))
                chunk["patent_abstract"] = chunk["patent_id"].map(abstract_map).fillna("")
            else:
                chunk["patent_abstract"] = ""

            chunk = chunk.rename(columns={
                "patent_title": "title",
                "patent_abstract": "abstract",
                "patent_date": "filing_date",
                "patent_type": "patent_type",
                "wipo_kind": "wipo_kind",
            })

            chunk["filing_date"] = pd.to_datetime(chunk["filing_date"], errors="coerce")
            chunk["year"] = chunk["filing_date"].dt.year.astype("Int64")

            before = len(chunk)
            chunk = chunk.dropna(subset=["patent_id", "filing_date"])
            total_dropped += (before - len(chunk))

            chunk["title"] = chunk["title"].astype(str).str.strip()
            chunk["abstract"] = chunk["abstract"].fillna("").astype(str).str.strip()
            chunk["patent_type"] = chunk["patent_type"].fillna("").astype(str).str.strip().str.lower()

            chunk = chunk[["patent_id", "title", "abstract", "filing_date", "year", "patent_type", "wipo_kind"]]
            chunk["filing_date"] = chunk["filing_date"].dt.strftime("%Y-%m-%d")

            chunk.to_csv(out_path, index=False, mode="w" if first_write else "a", header=first_write)
            first_write = False
            total_saved += len(chunk)

        if lookup_conn is not None:
            lookup_db = CLEAN_DIR / "_abstract_lookup.sqlite"
            lookup_conn.close()
            if lookup_db.exists():
                lookup_db.unlink()

        print(f"  Dropped {total_dropped:,} rows with missing id/date")
        print(f"  ✓ Saved  {out_path}  ({total_saved:,} rows)\n")
        return None

    df = read_tsv(
        "g_patent.tsv",
        usecols=["patent_id", "patent_title", "patent_date", "patent_type", "wipo_kind"],
        nrows=SAMPLE_ROWS,
    )

    # Ensure stable join/filter keys across mixed-source tables.
    df["patent_id"] = df["patent_id"].astype(str).str.strip()

    df["patent_abstract"] = ""
    if "patent_type" not in df.columns:
        df["patent_type"] = ""
    if "wipo_kind" not in df.columns:
        df["wipo_kind"] = ""

    abstract_file = RAW_DIR / "g_patent_abstract.tsv"
    if abstract_file.exists():
        valid_patent_ids = set(df["patent_id"])
        abstract_chunks = []
        for abstract_df in iter_tsv_chunks(
            "g_patent_abstract.tsv",
            usecols=["patent_id", "patent_abstract"],
        ):
            abstract_df["patent_id"] = abstract_df["patent_id"].astype(str).str.strip()
            abstract_df = abstract_df[abstract_df["patent_id"].isin(valid_patent_ids)]
            if not abstract_df.empty:
                abstract_chunks.append(abstract_df)

        if abstract_chunks:
            abstract_df = pd.concat(abstract_chunks, ignore_index=True)
            abstract_df = abstract_df.drop_duplicates(subset=["patent_id"])
            # Use map instead of merge to avoid dtype coercion issues
            abstract_dict = dict(zip(abstract_df["patent_id"], abstract_df["patent_abstract"]))
            df["patent_abstract"] = df["patent_id"].map(abstract_dict).fillna("")
        else:
            print("    [INFO] g_patent_abstract.tsv found but no matching sampled patent abstracts")
    else:
        print("    [INFO] g_patent_abstract.tsv not found; abstracts will be empty")

    # Rename columns to match our schema
    df = df.rename(columns={
        "patent_title":    "title",
        "patent_abstract": "abstract",
        "patent_date":     "filing_date",
        "patent_type":     "patent_type",
        "wipo_kind":       "wipo_kind",
    })

    # Fix dates → datetime, then extract year
    df["filing_date"] = pd.to_datetime(df["filing_date"], errors="coerce")
    df["year"] = df["filing_date"].dt.year.astype("Int64")

    # Drop rows with no patent_id or no date
    before = len(df)
    df = df.dropna(subset=["patent_id", "filing_date"])
    print(f"  Dropped {before - len(df):,} rows with missing id/date")

    # Clean text fields
    df["title"]    = df["title"].astype(str).str.strip()
    df["abstract"] = df["abstract"].fillna("").astype(str).str.strip()

    # Keep all patent types to preserve full PatentsView row counts.
    df["patent_type"] = df["patent_type"].fillna("").astype(str).str.strip().str.lower()

    # Final column order
    df = df[["patent_id", "title", "abstract", "filing_date", "year", "patent_type", "wipo_kind"]]
    df["filing_date"] = df["filing_date"].dt.strftime("%Y-%m-%d")

    save_clean(df, "patents")
    return df


# ─── 2. Inventors ─────────────────────────────────────────────────────────────
def clean_inventors() -> pd.DataFrame:
    print("── Inventors ────────────────────────────────────────────────")
    if SAMPLE_ROWS is None:
        out_path = CLEAN_DIR / "clean_inventors.csv"
        if out_path.exists():
            out_path.unlink()

        seen_ids = set()
        first_write = True
        total_saved = 0
    chunks = []
    loc_map = load_location_country_map()

    for df in iter_tsv_chunks(
        "g_inventor_disambiguated.tsv",
        usecols=[
            "disambig_inventor_id",
            "inventor_id",
            "disambig_inventor_name_first",
            "disambig_inventor_name_last",
            "inventor_country",
            "location_id",
        ],
    ):
        if "disambig_inventor_id" in df.columns:
            id_col = "disambig_inventor_id"
        elif "inventor_id" in df.columns:
            id_col = "inventor_id"
        else:
            raise ValueError("No inventor id column found in g_inventor_disambiguated.tsv")

        if "inventor_country" not in df.columns:
            df["inventor_country"] = None
        if "location_id" not in df.columns:
            df["location_id"] = None

        if not loc_map.empty:
            df = df.merge(loc_map, on="location_id", how="left")
        else:
            df["country_from_location"] = None

        country_series = normalize_country_series(
            df["inventor_country"].where(
                df["inventor_country"].notna() & (df["inventor_country"].astype(str).str.strip() != ""),
                df["country_from_location"],
            )
        )

        first_name = df["disambig_inventor_name_first"].fillna("").astype(str).str.strip()
        last_name = df["disambig_inventor_name_last"].fillna("").astype(str).str.strip()
        name = (first_name + " " + last_name).str.strip()

        out = pd.DataFrame({
            "inventor_id": df[id_col],
            "name": name,
            "country": country_series,
        })
        out = out.dropna(subset=["inventor_id"])
        out = out[out["name"].str.len() > 1]
        out["inventor_id"] = out["inventor_id"].astype(str).str.strip()

        if SAMPLE_ROWS is None:
            out = out[~out["inventor_id"].isin(seen_ids)]
            seen_ids.update(out["inventor_id"].tolist())
            out.to_csv(out_path, index=False, mode="w" if first_write else "a", header=first_write)
            first_write = False
            total_saved += len(out)
        else:
            chunks.append(out)

    if SAMPLE_ROWS is None:
        print(f"  ✓ Saved  {out_path}  ({total_saved:,} rows)\n")
        return None

    out = pd.concat(chunks, ignore_index=True).drop_duplicates(subset=["inventor_id"])

    save_clean(out, "inventors")
    return out


# ─── 3. Companies (Assignees) ─────────────────────────────────────────────────
def clean_companies() -> pd.DataFrame:
    print("── Companies (Assignees) ────────────────────────────────────")
    if SAMPLE_ROWS is None:
        out_path = CLEAN_DIR / "clean_companies.csv"
        if out_path.exists():
            out_path.unlink()

        seen_ids = set()
        first_write = True
        total_saved = 0
    chunks = []
    loc_map = load_location_country_map()

    for df in iter_tsv_chunks(
        "g_assignee_disambiguated.tsv",
        usecols=[
            "disambig_assignee_id",
            "assignee_id",
            "disambig_assignee_organization",
            "assignee_country",
            "location_id",
        ],
    ):
        if "disambig_assignee_id" in df.columns:
            id_col = "disambig_assignee_id"
        elif "assignee_id" in df.columns:
            id_col = "assignee_id"
        else:
            raise ValueError("No assignee id column found in g_assignee_disambiguated.tsv")

        if "assignee_country" not in df.columns:
            df["assignee_country"] = None
        if "location_id" not in df.columns:
            df["location_id"] = None

        if not loc_map.empty:
            df = df.merge(loc_map, on="location_id", how="left")
        else:
            df["country_from_location"] = None

        country_series = normalize_country_series(
            df["assignee_country"].where(
                df["assignee_country"].notna() & (df["assignee_country"].astype(str).str.strip() != ""),
                df["country_from_location"],
            )
        )

        out = pd.DataFrame({
            "company_id": df[id_col],
            "name": df["disambig_assignee_organization"],
            "country": country_series,
        })
        out = out.dropna(subset=["company_id", "name"])
        out["name"] = out["name"].astype(str).str.strip()
        out["company_id"] = out["company_id"].astype(str).str.strip()

        if SAMPLE_ROWS is None:
            out = out[~out["company_id"].isin(seen_ids)]
            seen_ids.update(out["company_id"].tolist())
            out.to_csv(out_path, index=False, mode="w" if first_write else "a", header=first_write)
            first_write = False
            total_saved += len(out)
        else:
            chunks.append(out)

    if SAMPLE_ROWS is None:
        print(f"  ✓ Saved  {out_path}  ({total_saved:,} rows)\n")
        return None

    out = pd.concat(chunks, ignore_index=True)

    # De-duplicate
    out = out.drop_duplicates(subset=["company_id"])
    out = out[out["name"].str.len() > 1]

    save_clean(out, "companies")
    return out


# ─── 4. Relationships ────────────────────────────────────────────────────────
def clean_relationships(patents_df: pd.DataFrame | None) -> None:
    print("── Relationships ────────────────────────────────────────────")
    valid_patents = set(patents_df["patent_id"].astype(str)) if patents_df is not None else None

    # In full-load mode, avoid giant cross-join by writing inventor and company links as separate rows.
    if valid_patents is None:
        out_path = CLEAN_DIR / "clean_relationships.csv"
        if out_path.exists():
            out_path.unlink()

        first_write = True
        total_saved = 0

        for pi in iter_tsv_chunks(
            "g_inventor_disambiguated.tsv",
            usecols=["patent_id", "inventor_id", "disambig_inventor_id"],
        ):
            if "inventor_id" not in pi.columns and "disambig_inventor_id" in pi.columns:
                pi["inventor_id"] = pi["disambig_inventor_id"]
            pi = pi[["patent_id", "inventor_id"]].dropna().drop_duplicates()
            pi["company_id"] = None
            pi = pi[["patent_id", "inventor_id", "company_id"]]
            pi.to_csv(out_path, index=False, mode="w" if first_write else "a", header=first_write)
            first_write = False
            total_saved += len(pi)

        for pa in iter_tsv_chunks(
            "g_assignee_disambiguated.tsv",
            usecols=["patent_id", "assignee_id", "disambig_assignee_id"],
        ):
            if "assignee_id" not in pa.columns and "disambig_assignee_id" in pa.columns:
                pa["assignee_id"] = pa["disambig_assignee_id"]
            pa = pa.rename(columns={"assignee_id": "company_id"})
            pa = pa[["patent_id", "company_id"]].dropna().drop_duplicates()
            pa["inventor_id"] = None
            pa = pa[["patent_id", "inventor_id", "company_id"]]
            pa.to_csv(out_path, index=False, mode="a", header=False)
            total_saved += len(pa)

        print(f"  ✓ Saved  {out_path}  ({total_saved:,} rows)\n")
        return

    # patent ↔ inventor (from dedicated mapping file when available, otherwise from inventor table)
    patent_inventor_file = "g_patent_inventor.tsv"
    inventor_source_file = patent_inventor_file if (RAW_DIR / patent_inventor_file).exists() else "g_inventor_disambiguated.tsv"
    pi_chunks = []
    for pi in iter_tsv_chunks(
        inventor_source_file,
        usecols=["patent_id", "inventor_id", "disambig_inventor_id"],
    ):
        if "inventor_id" not in pi.columns and "disambig_inventor_id" in pi.columns:
            pi["inventor_id"] = pi["disambig_inventor_id"]
        pi = pi[["patent_id", "inventor_id"]]
        if valid_patents is not None:
            pi = pi[pi["patent_id"].astype(str).isin(valid_patents)]
        pi = pi.dropna().drop_duplicates()
        pi_chunks.append(pi)
    pi = pd.concat(pi_chunks, ignore_index=True).drop_duplicates()

    # patent ↔ assignee (from dedicated mapping file when available, otherwise from assignee table)
    patent_assignee_file = "g_patent_assignee.tsv"
    assignee_source_file = patent_assignee_file if (RAW_DIR / patent_assignee_file).exists() else "g_assignee_disambiguated.tsv"
    pa_chunks = []
    for pa in iter_tsv_chunks(
        assignee_source_file,
        usecols=["patent_id", "assignee_id", "disambig_assignee_id"],
    ):
        if "assignee_id" not in pa.columns and "disambig_assignee_id" in pa.columns:
            pa["assignee_id"] = pa["disambig_assignee_id"]
        pa = pa.rename(columns={"assignee_id": "company_id"})
        pa = pa[["patent_id", "company_id"]]
        if valid_patents is not None:
            pa = pa[pa["patent_id"].astype(str).isin(valid_patents)]
        pa = pa.dropna().drop_duplicates()
        pa_chunks.append(pa)
    pa = pd.concat(pa_chunks, ignore_index=True).drop_duplicates()

    # Merge into one relationships table (outer join on patent_id)
    rel = pd.merge(pi, pa, on="patent_id", how="outer")
    rel = rel.dropna(subset=["patent_id"])

    save_clean(rel, "relationships")


# ─── Main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("  PatentsView Data Cleaner")
    print("=" * 60)

    patents   = clean_patents()

    inventors_file = CLEAN_DIR / "clean_inventors.csv"
    companies_file = CLEAN_DIR / "clean_companies.csv"
    relationships_file = CLEAN_DIR / "clean_relationships.csv"

    if SAMPLE_ROWS is None and inventors_file.exists() and companies_file.exists() and relationships_file.exists():
        print("  [INFO] Full patent refresh mode: reusing existing clean_inventors.csv, clean_companies.csv, and clean_relationships.csv")
    else:
        inventors = clean_inventors()
        companies = clean_companies()
        clean_relationships(patents)

    print("=" * 60)
    print("Cleaning complete.")
    print(f"Clean files saved to: {CLEAN_DIR.resolve()}")
    
