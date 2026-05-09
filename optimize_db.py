"""Add database indexes to speed up dashboard queries."""
import sqlite3
from project_paths import DB_PATH

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Create indexes on frequently queried columns
indexes = [
    ("idx_patents_title", "CREATE INDEX IF NOT EXISTS idx_patents_title ON patents(title)"),
    ("idx_patents_year", "CREATE INDEX IF NOT EXISTS idx_patents_year ON patents(year)"),
    ("idx_rel_patent_id", "CREATE INDEX IF NOT EXISTS idx_rel_patent_id ON relationships(patent_id)"),
    ("idx_rel_inventor_id", "CREATE INDEX IF NOT EXISTS idx_rel_inventor_id ON relationships(inventor_id)"),
    ("idx_rel_company_id", "CREATE INDEX IF NOT EXISTS idx_rel_company_id ON relationships(company_id)"),
]

for idx_name, create_sql in indexes:
    try:
        cursor.execute(create_sql)
        print(f"✓ Created index: {idx_name}")
    except Exception as e:
        print(f"✗ Error creating {idx_name}: {e}")

conn.commit()
conn.close()
print("\nDatabase optimization complete.")
