WITH facts AS (
    SELECT * FROM {{ ref('fact_complaints') }}
    -- Only train on records that actually have text!
    WHERE has_narrative = TRUE AND complaint_narrative IS NOT NULL
),

products AS (
    SELECT * FROM {{ ref('dim_products') }}
)

SELECT
    f.complaint_narrative AS text_input,
    p.product_category AS target_label
FROM facts f
LEFT JOIN products p ON f.product_id = p.product_id