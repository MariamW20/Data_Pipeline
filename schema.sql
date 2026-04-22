
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
