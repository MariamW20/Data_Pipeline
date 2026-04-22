"""
Step 2: Clean raw PatentsView TSV files with pandas.
Produces clean CSVs ready for loading into SQLite.
"""

import os
import pandas as pd
from project_paths import CLEAN_DIR, RAW_DIR, ensure_directories

try:
    import pycountry
except Exception:
    pycountry = None

# ─── Paths ────────────────────────────────────────────────────────────────────
ensure_directories()

# Limit rows while developing/testing  (set to None for full dataset)
SAMPLE_ROWS = 200_000   # ~200 K patents — fast and representative


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
        dtype=str,
        chunksize=chunksize,
        engine="python",
        on_bad_lines="skip",
    )


def save_clean(df: pd.DataFrame, name: str) -> None:
    path = CLEAN_DIR / f"clean_{name}.csv"
    df.to_csv(path, index=False)
    print(f"  ✓ Saved  {path}  ({len(df):,} rows)\n")


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
    df = read_tsv(
        "g_patent.tsv",
        usecols=["patent_id", "patent_title", "patent_abstract",
                 "patent_date", "patent_type", "wipo_kind"],
        nrows=SAMPLE_ROWS,
    )

    if "patent_abstract" not in df.columns:
        df["patent_abstract"] = ""
    if "patent_type" not in df.columns:
        df["patent_type"] = ""
    if "wipo_kind" not in df.columns:
        df["wipo_kind"] = ""

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

    # Keep only utility patents (most common; ignore design/plant)
    df["patent_type"] = df["patent_type"].fillna("").astype(str).str.strip().str.lower()
    df = df[df["patent_type"].isin(["utility", "reissue", ""])]

    # Final column order
    df = df[["patent_id", "title", "abstract", "filing_date", "year", "patent_type", "wipo_kind"]]
    df["filing_date"] = df["filing_date"].dt.strftime("%Y-%m-%d")

    save_clean(df, "patents")
    return df


# ─── 2. Inventors ─────────────────────────────────────────────────────────────
def clean_inventors() -> pd.DataFrame:
    print("── Inventors ────────────────────────────────────────────────")
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
        chunks.append(out)

    out = pd.concat(chunks, ignore_index=True).drop_duplicates(subset=["inventor_id"])

    save_clean(out, "inventors")
    return out


# ─── 3. Companies (Assignees) ─────────────────────────────────────────────────
def clean_companies() -> pd.DataFrame:
    print("── Companies (Assignees) ────────────────────────────────────")
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
        chunks.append(out)

    out = pd.concat(chunks, ignore_index=True)

    # De-duplicate
    out = out.drop_duplicates(subset=["company_id"])
    out = out[out["name"].str.len() > 1]

    save_clean(out, "companies")
    return out


# ─── 4. Relationships ────────────────────────────────────────────────────────
def clean_relationships(patents_df: pd.DataFrame) -> None:
    print("── Relationships ────────────────────────────────────────────")
    valid_patents = set(patents_df["patent_id"].astype(str))

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
    inventors = clean_inventors()
    companies = clean_companies()
    clean_relationships(patents)

    print("=" * 60)
    print("✅  Cleaning complete.")
    print(f"   Clean files saved to: {CLEAN_DIR.resolve()}")
    print("   Next step → run  03_load_database.py")
