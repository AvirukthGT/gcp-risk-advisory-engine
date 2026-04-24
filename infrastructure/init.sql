-- Creating the landing table for the raw API stream
CREATE TABLE IF NOT EXISTS cfpb_complaints (
    complaint_id VARCHAR(50) PRIMARY KEY,
    date_received TIMESTAMPTZ,
    date_sent_to_company TIMESTAMPTZ,
    product VARCHAR(255),
    sub_product VARCHAR(255),
    issue VARCHAR(255),
    sub_issue VARCHAR(255),
    company VARCHAR(255),
    state VARCHAR(5),
    zip_code VARCHAR(20),
    submitted_via VARCHAR(50),
    company_response VARCHAR(255),
    company_public_response TEXT,
    timely VARCHAR(10),
    consumer_disputed VARCHAR(10),
    consumer_consent_provided VARCHAR(100),
    has_narrative BOOLEAN,
    tags VARCHAR(100),
    complaint_what_happened TEXT, -- Stored as TEXT because consumers write long essays
    
    -- Metadata columns for auditing
    ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Index on date for faster CDC querying later
CREATE INDEX idx_date_received ON cfpb_complaints(date_received);