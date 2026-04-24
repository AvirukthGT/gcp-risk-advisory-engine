WITH staging AS (

    -- Reading from Silver layer, not the raw Bronze data.
    SELECT * FROM {{ ref('stg_cfpb_complaints') }}
),

company_stats AS (
    -- Aggregate to ensure we get exactly one row per company
    SELECT
        company as company_name,
        MIN(received_date) AS first_complaint_date,
        MAX(received_date) AS latest_complaint_date,
        COUNT(complaint_id) AS total_complaints
    FROM staging
    WHERE company IS NOT NULL
    GROUP BY 1
)

SELECT
    -- Enterprise standard: Generating a Surrogate Key (a unique ID) using a Hash
    TO_HEX(MD5(company_name)) AS company_id,
    company_name,
    first_complaint_date,
    latest_complaint_date,
    total_complaints
FROM company_stats