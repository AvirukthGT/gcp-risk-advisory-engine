import os
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta

try:
    from dotenv import load_dotenv
    # load env vars so we don't commit secrets again lol
    load_dotenv()
except ImportError:
    pass

DB_CONN = os.getenv("DB_CONN")

def get_target_date_range():
    # getting a specific date range, cfpb has a lag so we grab older data to seed
    return "2025-10-01", "2025-10-31"

def fetch_and_load():
    start_date, end_date = get_target_date_range()
    print(f"Starting historical extraction from {start_date} to {end_date}")

    base_url = "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/"
    size = 1000
    offset = 0
    total_inserted = 0

    # connecting to local postgres
    conn = psycopg2.connect(DB_CONN)
    cursor = conn.cursor()

    while True:
        # elasticsearch max result window is 50k, gotta break before it loops forever
        if offset >= 50000:
            print("Reached Elasticsearch pagination limit (50,000 records). Halting extraction.")
            break

        print(f"Fetching records {offset} to {offset + size}...")

        # update params for date range
        params = {
            "date_received_min": start_date,
            "date_received_max": end_date,
            "size": size,
            "from": offset,
            "has_narrative": "true"
        }

        response = requests.get(base_url, params=params)
        response.raise_for_status() # fail loud if api is down
        data = response.json()

        hits = data.get("hits", {}).get("hits", [])

        if not hits:
            print("No more records found for this date range.")
            break

        # parse into tuples for psycopg2 bulk insert
        records_to_insert = []
        for hit in hits:
            source = hit["_source"]
            record = (
                source.get("complaint_id"),
                source.get("date_received"),
                source.get("date_sent_to_company"),
                source.get("product"),
                source.get("sub_product"),
                source.get("issue"),
                source.get("sub_issue"),
                source.get("company"),
                source.get("state"),
                source.get("zip_code"),
                source.get("submitted_via"),
                source.get("company_response"),
                source.get("company_public_response"),
                source.get("timely"),
                source.get("consumer_disputed"),
                source.get("consumer_consent_provided"),
                source.get("has_narrative"),
                source.get("tags"),
                source.get("complaint_what_happened")
            )
            records_to_insert.append(record)

        # doing bulk insert, added on conflict do nothing so i can rerun this safely
        insert_query = """
            INSERT INTO cfpb_complaints (
                complaint_id, date_received, date_sent_to_company, product, sub_product,
                issue, sub_issue, company, state, zip_code, submitted_via,
                company_response, company_public_response, timely, consumer_disputed,
                consumer_consent_provided, has_narrative, tags, complaint_what_happened
            ) VALUES %s
            ON CONFLICT (complaint_id) DO NOTHING;
        """

        # run the insert
        execute_values(cursor, insert_query, records_to_insert)
        conn.commit()

        total_inserted += len(records_to_insert)
        offset += size

        # break if we got less than chunk size
        if len(hits) < size:
            break

    cursor.close()
    conn.close()
    print(f"Extraction complete. Successfully inserted {total_inserted} records into legacy database.")

if __name__ == "__main__":
    fetch_and_load()