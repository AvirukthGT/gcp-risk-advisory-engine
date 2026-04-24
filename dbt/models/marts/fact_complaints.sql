WITH staging AS (
    SELECT * FROM {{ ref('stg_cfpb_complaints') }}
)

SELECT
    -- 1. Primary Key
    complaint_id,
    
    -- 2. Foreign Keys (Connecting to your Dimensions)
    -- Using the EXACT same hashing logic here to create the Foreign Keys. 
    -- This is called "Deterministic Hashing" and it avoids massive, slow JOIN operations.
    TO_HEX(MD5(company)) AS company_id,
    TO_HEX(MD5(CONCAT(product_category, '|', COALESCE(sub_product, 'NONE')))) AS product_id,
    
    -- 3. Dimensions specific to the event (Degenerate Dimensions)
    state,
    zip_code,
    submitted_via,
    
    -- 4. Dates
    received_date,
    sent_to_company_date,
    
    -- 5. Metrics & Flags (The actual measurable data)
    is_timely_response,
    has_narrative,
    
    -- We keep the narrative here for NLP/Vertex AI analysis later
    complaint_narrative

FROM staging