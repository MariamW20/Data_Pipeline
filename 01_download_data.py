"""
Step 1: Download patent data from PatentsView (USPTO).
This script downloads the TSV files and saves them locally.
"""

import io
import os
import zipfile

import requests
from project_paths import RAW_DIR, ensure_directories

# Configuration
ensure_directories()
DATA_DIR = str(RAW_DIR)

# PatentsView bulk data base URL (granted patents, disambiguated)
BASE_URL = "https://s3.amazonaws.com/data.patentsview.org/download/"

# Files to download - these are the tables we need
# Full filenames from the PatentsView bulk download page
FILES = {
    "patent": "g_patent.tsv.zip",
    "inventor": "g_inventor_disambiguated.tsv.zip",
    "assignee": "g_assignee_disambiguated.tsv.zip",
    "patent_inventor": "g_persistent_inventor.tsv.zip",
    "patent_assignee": "g_persistent_assignee.tsv.zip",
    "location": "g_location_disambiguated.tsv.zip",
    "pct_data": "g_pct_data.tsv.zip",
}


def download_and_extract(name: str, filename: str) -> None:
    """Download a zip from PatentsView and extract the TSV inside."""
    out_path = os.path.join(DATA_DIR, filename.replace(".zip", ""))
    if os.path.exists(out_path):
        print(f"  [SKIP] {name} already downloaded -> {out_path}")
        return

    url = BASE_URL + filename
    print(f"  [GET]  {name} <- {url}")
    resp = requests.get(url, stream=True, timeout=120)
    resp.raise_for_status()

    total = int(resp.headers.get("content-length", 0))
    downloaded = 0
    chunks = []
    for chunk in resp.iter_content(chunk_size=1024 * 1024):  # 1 MB chunks
        chunks.append(chunk)
        downloaded += len(chunk)
        if total:
            pct = downloaded / total * 100
            print(
                f"\r    {pct:.1f}% ({downloaded // 1_000_000} MB / {total // 1_000_000} MB)",
                end="",
                flush=True,
            )
    print()

    raw = b"".join(chunks)
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        for member in archive.namelist():
            if member.endswith(".tsv"):
                archive.extract(member, DATA_DIR)
                extracted = os.path.join(DATA_DIR, member)
                if extracted != out_path:
                    os.rename(extracted, out_path)
                print(f"    -> Extracted: {out_path}")
                break


if __name__ == "__main__":
    print("=" * 60)
    print("  PatentsView Data Downloader")
    print("=" * 60)
    print(f"\nSaving raw files to: {os.path.abspath(DATA_DIR)}\n")

    for name, filename in FILES.items():
        download_and_extract(name, filename)

    print("\nAll files downloaded successfully.")
