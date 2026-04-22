"""Shared path resolution for the patent pipeline.

Supports either:
1) flat workspace layout (raw/, clean/, reports/, patents.db)
2) nested layout (data/raw, data/clean, reports/, data/patents.db)
"""

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent


def _first_existing(candidates: list[Path]) -> Path | None:
    for path in candidates:
        if path.exists():
            return path
    return None


RAW_DIR = _first_existing([
    PROJECT_ROOT / "raw",
    PROJECT_ROOT / "data" / "raw",
]) or (PROJECT_ROOT / "raw")

CLEAN_DIR = _first_existing([
    PROJECT_ROOT / "clean",
    PROJECT_ROOT / "data" / "clean",
]) or (PROJECT_ROOT / "clean")

REPORTS_DIR = _first_existing([
    PROJECT_ROOT / "reports",
]) or (PROJECT_ROOT / "reports")

DB_PATH = _first_existing([
    PROJECT_ROOT / "patents.db",
    PROJECT_ROOT / "data" / "patents.db",
]) or (PROJECT_ROOT / "patents.db")

SQL_DIR = _first_existing([
    PROJECT_ROOT / "sql",
    PROJECT_ROOT,
]) or PROJECT_ROOT

SCHEMA_SQL = SQL_DIR / "schema.sql"
QUERIES_SQL = SQL_DIR / "queries.sql"


def ensure_directories() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    SQL_DIR.mkdir(parents=True, exist_ok=True)
