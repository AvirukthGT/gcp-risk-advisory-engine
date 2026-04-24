#!/bin/bash

# ==========================================
# CONFIGURATION - UPDATE YOUR PROJECT ID
# ==========================================
PROJECT_ID="cfpb-risk"  
REGION="us-central1"
BUCKET_NAME="cfpb-raw-landing-${PROJECT_ID}" 

echo "========================================"
echo "Starting GCP Infrastructure Setup..."
echo "========================================"

echo "1. Setting active project to: $PROJECT_ID"
gcloud config set project $PROJECT_ID

echo "2. Enabling Cloud Storage and BigQuery APIs..."
gcloud services enable storage.googleapis.com bigquery.googleapis.com

echo "3. Creating GCS Data Lake Bucket: gs://$BUCKET_NAME"
gcloud storage buckets create gs://$BUCKET_NAME --location=$REGION --project=$PROJECT_ID

echo "4. Creating BigQuery Bronze Dataset: cfpb_bronze"
bq --location=$REGION mk -d ${PROJECT_ID}:cfpb_bronze

echo "========================================"
echo "Infrastructure Provisioned Successfully!"
echo "========================================"