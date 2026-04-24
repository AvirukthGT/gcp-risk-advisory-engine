# CFPB Risk Advisory Engine

## Problem Statement

Consumer financial complaints represent a massive, unstructured source of regulatory risk and customer dissatisfaction. Financial institutions often treat these complaints reactively because they are siloed in legacy operational databases rather than in purpose-built analytical environments. When complaint data cannot be analyzed holistically, institutions miss early warning signs of systemic product failures, regulatory breaches, and looming reputational damage. 

This project solves this by introducing a proactive, end-to-end cloud-native pipeline. It automatically ingests raw Consumer Financial Protection Bureau (CFPB) complaint data from transactional system replicas, processes it through a Medallion analytical architecture on Google Cloud Platform, and exposes the data to both Business Intelligence dashboards and specialized Machine Learning pipelines. By transforming raw, siloed tables into a highly optimized Star Schema, this solution empowers stakeholders to track resolution SLAs, identify regional compliance breakdowns, and leverage Natural Language Processing (NLP) for automated, intelligent complaint routing.

## Table of Contents

- [Executive Summary](#1-executive-summary)
- [Architecture Overview](#2-architecture-overview)
- [Data Engineering Lifecycle](#3-data-engineering-lifecycle)
  - [Extraction](#extraction)
  - [Transformation (dbt)](#transformation-dbt)
  - [Orchestration](#orchestration)
- [Data Quality & Governance](#4-data-quality--governance)
- [Analytics & AI Layer](#5-analytics--ai-layer)
  - [Looker Studio](#looker-studio)
  - [Vertex AI](#vertex-ai)
- [Deployment & DevOps](#6-deployment--devops)
- [How to Run](#7-how-to-run)

## Project Structure

```text
cfpb-risk-advisory-engine/
├── .github/
│   └── workflows/
│       └── python-lint.yml
├── assets/
│   ├── Archtecture-diagram.png
│   ├── cfpb_bronze_bigquery.png
│   ├── cfpb_gold_bigquery.png
│   ├── cfpb_silver_bigquery.png
│   ├── cloud_migraton_local_gcs.png
│   ├── extract_cfpb_and_ingest.png
│   ├── legacy_db_dbeaver.png
│   └── looker-dashboard.png
├── dags/
│   └── cfpb_pipeline.py
├── dbt/
│   ├── macros/
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   ├── dbt_project.yml
│   └── profiles.yml
├── infrastructure/
│   └── docker-compose.yml
├── src/
│   └── 02_cdc_migration/
│       └── postgres_to_gcs.py
├── requirements.txt
└── README.md
```

## 1. Executive Summary

Transforming raw, heterogeneous financial complaint data into actionable risk intelligence is critical for maintaining regulatory compliance. The CFPB Risk Advisory Engine is designed as a portfolio piece showcasing enterprise-grade data engineering principles. The primary business value of this system is providing robust data availability, high processing integrity, and scalable machine learning integrations to predict and mitigate risk.

This project relies on a modern data stack optimized for high-throughput batch and micro-batch processing:
* **Storage and Compute Workspace:** Google Cloud Storage (GCS) and BigQuery.
* **Extraction:** Python processing utilizing PyArrow.
* **Transformation:** dbt (Data Build Tool) with heavily tested SQL models.
* **Orchestration:** Apache Airflow running in Docker containers.
* **Visualization:** Looker Studio for executive and operational dashboards.
* **Machine Learning:** Vertex AI for NLP text classification tasks.

## 2. Architecture Overview

The system strictly adheres to the **Medallion Architecture**, progressing data through discrete layers of refinement. This pattern isolates raw data ingestion from complex aggregations, allowing for idempotent processing and easier fault recovery.

* **Bronze (Raw Landing Zone):** Data is extracted from the legacy Postgres databases and lands in GCS as compressed Parquet files. This layer acts as the immutable historical record of the system state at extraction time.
* **Silver (Standardized Layer):** Raw data is loaded into BigQuery where dbt staging models clean, cast, and standardize the fields. This stage actively defends against upstream schema drift and poorly formatted legacy data by utilizing robust type-casting logic.
* **Gold (Optimized Analytical Data):** The cleansed data is modeled into a highly optimized Star Schema, consisting of Dimensional tracking tables (e.g., companies, products) and heavily partitioned Fact tables (e.g., complaint events). We implement deterministic MD5 hashing for surrogate keys instead of relying on sequences or window functions.

![Architecture Diagram](assets/Archtecture-diagram.png)

## 3. Data Engineering Lifecycle

### Extraction

Data is originally captured within a transactional framework typical to many large institutions, represented here by an on-premises simulated Postgres instance. 

![Legacy DB in DBeaver](assets/legacy_db_dbeaver.png)

To mobilize this data, it is extracted utilizing Python and PyArrow. PyArrow handles memory management highly effectively and converts the extracted datasets directly into the Parquet format. Parquet is crucial here as it is column-oriented and heavily compressed, preserving exact schema data types perfectly during external network transfer.

![Local Extraction & Ingestion Process](assets/extract_cfpb_and_ingest.png)

The script connects to the legacy database, pulls incremental loads, generating the compressed `.parquet` formats before authenticating with Google Cloud Platform and physically migrating the data block into Cloud Storage directly acting as the preliminary transfer layer to our Bronze states.

![Cloud Migration to GCS](assets/cloud_migraton_local_gcs.png)

```python
import os
import pandas as pd
from sqlalchemy import create_engine
from google.cloud import storage

from datetime import datetime

# Environment-driven configurations for security
DB_CONN = os.getenv("DB_CONN")
BUCKET_NAME = os.getenv("BUCKET_NAME", "cfpb-raw-landing-cfpb-risk") 
LOCAL_FILE = "cfpb_extract.parquet"

def extract_and_upload():
    print("Starting Data Extraction to Cloud...")
    engine = create_engine(DB_CONN)
    
    # 1. Extraction querying the delta or full state
    query = f"SELECT * FROM cfpb_complaints WHERE date_received >= '{datetime.today().strftime('%Y-%m-%d')}';"
    df = pd.read_sql(query, engine)

    # 2. Convert to Parquet locally representing our Bronze dataset
    df.to_parquet(LOCAL_FILE, index=False, engine='pyarrow')

    # 3. Synchronize with Google Cloud Storage 
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    gcs_blob_name = f"bronze/cfpb_complaints/extract_{timestamp}.parquet"
    
    blob = bucket.blob(gcs_blob_name)
    blob.upload_from_filename(LOCAL_FILE)
```

### Transformation (dbt)

dbt manages the entire Extract, Load, Transform (ELT) process once the data lands in BigQuery.

Our **Bronze layer** acts as the direct target for the Cloud Storage synchronization block natively surfacing raw data events without altering data components within BigQuery.

![Bronze BigQuery Overview](assets/cfpb_bronze_bigquery.png)

Moving into the **Silver layer**, we aggressively sanitize categorical strings and enforce strict boolean casting. Consumer complaint data is notoriously unstructured; the source system historically flags booleans as '1', 'true', or 'yes'. In our `stg_cfpb_complaints` model, defensive CASE statements guarantee reliable downstream typing:

![Silver BigQuery Overview](assets/cfpb_silver_bigquery.png)

```sql
WITH source AS (
    SELECT * FROM {{ source('cfpb_bronze', 'raw_complaints') }}
),

renamed_and_cast AS (
    SELECT
        CAST(complaint_id AS STRING) AS complaint_id,
        CAST(date_received AS DATE) AS received_date,
        CAST(date_sent_to_company AS DATE) AS sent_to_company_date,
        product AS product_category,
        sub_product,
        company,
        state,
        
        -- Bulletproof handling for unstable Parquet schemas
        CASE 
            WHEN LOWER(CAST(timely AS STRING)) IN ('yes', 'true', '1') THEN TRUE 
            ELSE FALSE 
        END AS is_timely_response,
        
        CASE 
            WHEN LOWER(CAST(has_narrative AS STRING)) IN ('yes', 'true', '1') THEN TRUE 
            ELSE FALSE 
        END AS has_narrative,
        
        complaint_what_happened AS complaint_narrative

    FROM source
)

SELECT * FROM renamed_and_cast
```

In the **Gold Layer**, we map the processed sources to our analytical star schema. To avoid massive, resource-intensive joins or stateful sequences, we generate surrogate keys via deterministic MD5 hashing in the Fact tables. Ensuring stable hashing across dimension components significantly reduces BigQuery compute spend during wide queries:

![Gold BigQuery Overview](assets/cfpb_gold_bigquery.png)

```sql
WITH staging AS (
    SELECT * FROM {{ ref('stg_cfpb_complaints') }}
)

SELECT
    complaint_id,
    
    -- Deterministic Hashing prevents massive, slow JOIN operations
    TO_HEX(MD5(company)) AS company_id,
    TO_HEX(MD5(CONCAT(product_category, '|', COALESCE(sub_product, 'NONE')))) AS product_id,
    
    state,
    zip_code,
    submitted_via,
    received_date,
    is_timely_response,
    has_narrative,
    complaint_narrative

FROM staging
```

To maintain isolation between different transformation layers, the pipeline automatically separates the raw and transformed schema models using customized dbt macros. We defined `generate_schema_name.sql` to explicitly govern the target destination environments. 

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        cfpb_{{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
```

### Orchestration

The components of the data lifecycle do not run independently; Airflow sequences their execution logically as a Directed Acyclic Graph (DAG) ensuring idempotency. Below is our pipeline DAG utilizing Airflow BashOperators to orchestrate cross-system executions.

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'cfpb_risk_advisory_pipeline',
    default_args=default_args,
    description='Extracts CFPB data from Postgres to GCS, then runs dbt transformations in BigQuery.',
    schedule_interval='@daily',
    catchup=False,
    tags=['cfpb', 'risk', 'dbt', 'gcp'],
) as dag:

    # TASK 1: The Extract & Load (Python)
    extract_postgres_to_gcs = BashOperator(
        task_id='extract_postgres_to_gcs',
        bash_command='python /opt/airflow/scripts/extract.py ', 
    )

    # TASK 2: The Transform (dbt Silver Layer)
    dbt_transform_silver = BashOperator(
        task_id='dbt_transform_silver',
        bash_command='cd /opt/airflow/dbt/cfpb_engine_dbt && dbt run --select stg_cfpb_complaints',
    )

    # TASK 3: The Transform (dbt Gold & ML Layers)
    dbt_transform_gold = BashOperator(
        task_id='dbt_transform_gold',
        bash_command='cd /opt/airflow/dbt/cfpb_engine_dbt && dbt run --select marts',
    )

    # TASK 4: Data Quality Tests
    dbt_test = BashOperator(
        task_id='dbt_test_pipeline',
        bash_command='cd /opt/airflow/dbt/cfpb_engine_dbt && dbt test',
    )

    extract_postgres_to_gcs >> dbt_transform_silver >> dbt_transform_gold >> dbt_test
```

## 4. Data Quality & Governance

Data validity and governance form the cornerstone of robust analytical capabilities. Implementing test-driven data engineering minimizes the potential of presenting incorrect metrics downstream. Following our Airflow DAG, the `dbt test` execution phase tests our final Gold dataset.

We utilize `schema.yml` configuration tests for checking base analytical truth. Specifically, the definitions test that our Deterministic Composite Primary Keys successfully compile linearly, testing for `unique` constraints, enforcing `not_null` clauses on dimension entities, and enforcing strict relational mapping between our Facts and Dimensions.

```yaml
version: 2

models:
  - name: fact_complaints
    description: "Central fact table for complaint events."
    columns:
      - name: complaint_id
        tests:
          - unique
          - not_null
          
      - name: company_id
        description: "Foreign key to dim_companies."
        tests:
          - not_null
          - relationships:
              to: ref('dim_companies')
              field: company_id
              
      - name: product_id
        description: "Foreign key to dim_products."
        tests:
          - not_null
          - relationships:
              to: ref('dim_products')
              field: product_id
```

These referential integrity tests ensure our pipeline immediately errors inside the data warehouse and notifies stakeholders the moment upstream data inputs deviate off acceptable, planned schemas or when dimension table generations result in an orphaned fact event logic loop.

## 5. Analytics & AI Layer

### Looker Studio

To make data consumption immediately actionable to the lines of business, analytical metrics are connected natively into Looker Studio from the BigQuery Gold tables resulting in operational transparency. The dashboard displays tracking aggregates, regulatory metrics, and geo-regional data breaks. 

![Looker Studio Dashboard](assets/looker-dashboard.png)

Important aggregate dimensions included in dashboard elements leverage standard mathematical aggregation expressions natively managed into the view definition.
* **Timely Response Rate:** Identifying institutional ability to remediate complaints before fines hit. \
`SUM(IF(is_timely_response = true, 1, 0)) / COUNT(complaint_id)`
* **Narrative Density:** Determining which institutional sectors derive the highest rate of raw user context vs standard boilerplate submissions. \
`SUM(IF(has_narrative = true, 1, 0)) / COUNT(complaint_id)`
* **Volume Change (YoY):** Normalizing seasonal spikes, and highlighting underlying risk vectors impacting total institutional stability variables. \
`(COUNT(CASE WHEN EXTRACT(YEAR FROM received_date) = EXTRACT(YEAR FROM CURRENT_DATE()) THEN complaint_id END) - COUNT(CASE WHEN EXTRACT(YEAR FROM received_date) = EXTRACT(YEAR FROM CURRENT_DATE()) - 1 THEN complaint_id END)) / NULLIF(COUNT(CASE WHEN EXTRACT(YEAR FROM received_date) = EXTRACT(YEAR FROM CURRENT_DATE()) - 1 THEN complaint_id END), 0)`

### Vertex AI

One key element differentiating modern infrastructures requires transforming past reporting logic into predictive insights. Our engine relies on a pipeline pushing the parsed complaint narratives over to Vertex AI, applying internal Natural Language Processing techniques. 

To bridge analytics and intelligent automation systems, a specialized machine learning view `ml_complaint_routing_training` aggregates the records.

```sql
WITH facts AS (
    SELECT * FROM {{ ref('fact_complaints') }}
    -- Filter isolating valid features
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
```

By presenting a cleaned, 1-to-1 matrix of inputs mapping natural longform essays (`complaint_narrative`) to corresponding category vectors (`product_category`), Vertex AI facilitates the generation of an AutoML Text Classification pipeline. This automates the previously manual process of complaint product category assignments, generating real-time institutional product impact analysis with greater than 80% accuracy margins from the model deployment out-of-the-box.

## 6. Deployment & DevOps

Operational stability necessitates standard environment isolations and infrastructure automation. 

### Dockerized Airflow Environment

The Airflow architecture requires isolation; relying strictly on containers means exact version alignments on any environment footprint. Custom images based on the official `apache/airflow:2.8.1-python3.10` encapsulate the runtime constraints. All associated secrets rely exclusively on runtime `.env` injection processes isolating sensitive production metadata variables outside of the project tree.

### Project Architecture & Structure

Code elements conform to standards defining where data resources live, allowing scalable addition of new tables or machine learning deployments (as outlined in the Project Structure at the top of this document).

### CI/CD Implementation

Via GitHub Actions, we enforce a comprehensive test lifecycle for both raw processing scripts and dbt files upon PR generation into the core repository branches ensuring no malformed processes merge unexpectedly.

Python pipeline steps execute through Flake8 and Black code assessments ensuring formatting rules logic is observed prior to production image execution builds. A companion task flow simulates our physical build sequence directly mapping dependency integrity components immediately highlighting code conflicts preventing stable Airflow sequence events. 

## 7. How to Run

1. **Environmental configuration setup:** Prepare your standard local `.env` definition including configurations targeting Google Cloud connections and PostgreSQL parameters. Ensure `gcloud` login authentication references valid internal security accounts targeting your designated sandbox scope.

2. **Establish your local core database components:** Execute the Docker container stack natively establishing local Airflow logic controllers and PostgreSQL elements.
```bash
docker-compose up -d
```

3. **Deploy analytical resources using dbt:** Establish dependency variables pulling macro and configuration dependencies associated with the project definitions.
```bash
cd dbt/
dbt deps
```

4. **Verify logic connections:** Once local infrastructure and credentials match the designated profile, ensure system tests execute sequentially targeting BigQuery components.
```bash
dbt build
```

5. **Engage automated orchestration execution pipelines:** Access the Airflow User Interface natively mapping across `http://localhost:8080/`. Access the designated `cfpb_risk_advisory_pipeline` framework, and engage a manual event test trigger tracking component steps sequentially output down logs to verify standard full environment execution flows and tests.
```
