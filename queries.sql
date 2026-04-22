-- Q1_top_inventors
SELECT
            i.name                              AS inventor,
            i.country,
            COUNT(DISTINCT r.patent_id)         AS patent_count
        FROM relationships r
        JOIN inventors i ON r.inventor_id = i.inventor_id
        GROUP BY r.inventor_id
        ORDER BY patent_count DESC
        LIMIT 20;

────────────────────────────────────────────────────────────

-- Q2_top_companies
SELECT
            c.name                              AS company,
            c.country,
            COUNT(DISTINCT r.patent_id)         AS patent_count
        FROM relationships r
        JOIN companies c ON r.company_id = c.company_id
        GROUP BY r.company_id
        ORDER BY patent_count DESC
        LIMIT 20;

────────────────────────────────────────────────────────────

-- Q3_top_countries
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

────────────────────────────────────────────────────────────

-- Q4_patents_per_year
SELECT
            year,
            COUNT(*)                            AS patent_count
        FROM patents
        WHERE year IS NOT NULL
          AND year BETWEEN 1976 AND 2025
        GROUP BY year
        ORDER BY year;

────────────────────────────────────────────────────────────

-- Q5_join_query
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

────────────────────────────────────────────────────────────

-- Q6_cte_query
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

────────────────────────────────────────────────────────────

-- Q7_inventor_ranking
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

────────────────────────────────────────────────────────────

