from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# 1. Defining the default arguments for the DAG
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 20), # Set this to a few days ago so it can "catch up"
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 2. Instantiating the DAG
with DAG(
    'cfpb_risk_advisory_pipeline',
    default_args=default_args,
    description='Extracts CFPB data from Postgres to GCS, then runs dbt transformations in BigQuery.',
    schedule_interval='@daily', # Runs once a day at midnight
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
    # This runs the dbt tests to ensure no nulls or broken logic made it to production
    dbt_test = BashOperator(
        task_id='dbt_test_pipeline',
        bash_command='cd /opt/airflow/dbt/cfpb_engine_dbt && dbt test',
    )

    # 3. Defining the Dependency Chain (The Pipeline Flow)
    extract_postgres_to_gcs >> dbt_transform_silver >> dbt_transform_gold >> dbt_test