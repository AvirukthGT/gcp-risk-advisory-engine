import os
import pandas as pd
from sqlalchemy import create_engine
from google.cloud import storage
from datetime import datetime

try:
    from dotenv import load_dotenv
    # grab env vars so we don't leak creds again
    load_dotenv()
except ImportError:
    pass

# config pulled from env
DB_CONN = os.getenv("DB_CONN")
BUCKET_NAME = os.getenv("BUCKET_NAME", "cfpb-raw-landing-cfpb-risk") 
LOCAL_FILE = "cfpb_extract.parquet"

def extract_and_upload():
    print("Starting Data Extraction to Cloud...")
    
    # 1. extract from postgres
    print("1. Connecting to local Postgres database...")
    engine = create_engine(DB_CONN)
    
    # query all rows for the dev extract
    query = f"SELECT * FROM cfpb_complaints WHERE date_received >= '{datetime.today().strftime('%Y-%m-%d')}';"
    df = pd.read_sql(query, engine)
    print(f"   -> Successfully extracted {len(df)} records.")

    # 2. convert to parquet
    print("2. Converting data to Parquet format...")
    # parquet compresses way better and keeps the schema intact
    df.to_parquet(LOCAL_FILE, index=False, engine='pyarrow')
    print(f"   -> Saved locally as {LOCAL_FILE}")

    # 3. upload to gcs
    print(f"3. Authenticating with GCP and connecting to gs://{BUCKET_NAME}...")
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    
    # sticking a timestamp on the file so we don't overwrite older dumps
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    gcs_blob_name = f"bronze/cfpb_complaints/extract_{timestamp}.parquet"
    
    blob = bucket.blob(gcs_blob_name)
    blob.upload_from_filename(LOCAL_FILE)
    print(f"   -> Uploaded successfully to gs://{BUCKET_NAME}/{gcs_blob_name}")

    # 4. cleanup
    print("4. Cleaning up local temporary files...")
    os.remove(LOCAL_FILE)
    
    print("Cloud Migration Complete!")

if __name__ == "__main__":
    extract_and_upload()