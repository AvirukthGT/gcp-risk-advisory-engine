WITH source AS (
    
    SELECT * FROM {{ source('cfpb_bronze', 'raw_complaints') }}
),

renamed_and_cast AS (
    SELECT
        -- Primary Key
        CAST(complaint_id AS STRING) AS complaint_id,
        
        -- Dates (Converting strings to actual Date objects for time-series analysis)
        CAST(date_received AS DATE) AS received_date,
        CAST(date_sent_to_company AS DATE) AS sent_to_company_date,
        
        -- Categorical Dimensions
        product AS product_category,
        sub_product,
        issue,
        sub_issue,
        company,
        state,
        zip_code,
        submitted_via,
        company_response,
        
        -- Booleans (Bulletproof handling for unstable Parquet schemas)
        CASE 
            WHEN LOWER(CAST(timely AS STRING)) IN ('yes', 'true', '1') THEN TRUE 
            ELSE FALSE 
        END AS is_timely_response,
        
        CASE 
            WHEN LOWER(CAST(has_narrative AS STRING)) IN ('yes', 'true', '1') THEN TRUE 
            ELSE FALSE 
        END AS has_narrative,
        -- Text / NLP fields
        complaint_what_happened AS complaint_narrative

    FROM source
)

SELECT * FROM renamed_and_cast