WITH staging AS (
    SELECT * FROM {{ ref('stg_cfpb_complaints') }}
),

unique_products AS (
    -- Getting every unique combination of product and sub-product
    SELECT DISTINCT
        product_category,
        sub_product
    FROM staging
    WHERE product_category IS NOT NULL
)

SELECT
    -- Enterprise Data Modeling: Creating a Composite Surrogate Key.
    -- Using COALESCE because hashing a NULL value will break the MD5 function.
    TO_HEX(MD5(CONCAT(product_category, '|', COALESCE(sub_product, 'NONE')))) AS product_id,
    product_category,
    sub_product
FROM unique_products