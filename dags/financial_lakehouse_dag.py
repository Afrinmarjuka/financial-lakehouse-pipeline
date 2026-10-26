"""
Apache Airflow DAG: Automated Financial Lakehouse & Data Quality Pipeline
Orchestrates:
  1. Bronze Raw Ingestion
  2. Silver Cleansing, Deduplication & Partitioning
  3. PyTest Automated Data Quality Assertions
  4. Gold Analytical Aggregations
  5. Cost-Optimization Cleanup (Purging raw staging files > 7 days)
"""

from datetime import datetime, timedelta
import os
import glob
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Define project directories relative to Airflow root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def purge_old_bronze_staging_files(days_retention: int = 7):
    """
    Cost-control task: Purges raw Bronze ingestion payloads older than
    the retention threshold to eliminate unnecessary storage costs.
    """
    now = datetime.now()
    removed_count = 0
    for file_path in RAW_DIR.glob("*.json"):
        file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
        if (now - file_mtime).days >= days_retention:
            file_path.unlink()
            removed_count += 1
    print(f"Cost optimization complete: Purged {removed_count} staging files older than {days_retention} days.")


with DAG(
    dag_id="financial_lakehouse_pipeline",
    default_args=default_args,
    description="Automated Medallion Lakehouse with PyTest Quality Gates and Cost Cleanup",
    schedule_interval="0 2 * * *",  # Scheduled daily at 02:00 AM UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lakehouse", "medallion", "data_quality", "finance"],
) as dag:

    # Task 1: Ingest raw streaming market data to Bronze
    task_ingest_bronze = BashOperator(
        task_id="ingest_bronze_layer",
        bash_command=f"python {PROJECT_ROOT / 'src' / 'ingest_bronze.py'}",
    )

    # Task 2: Clean, validate schema, and deduplicate to Silver Parquet
    task_bronze_to_silver = BashOperator(
        task_id="process_silver_layer",
        bash_command=f"python {PROJECT_ROOT / 'src' / 'bronze_to_silver.py'}",
    )

    # Task 3: Quality Gate - Run automated pytest data quality suite
    task_quality_gate = BashOperator(
        task_id="run_data_quality_tests",
        bash_command=f"pytest -v {PROJECT_ROOT / 'tests' / 'test_data_quality.py'}",
    )

    # Task 4: Aggregate business metrics into Gold Parquet
    task_silver_to_gold = BashOperator(
        task_id="process_gold_layer",
        bash_command=f"python {PROJECT_ROOT / 'src' / 'silver_to_gold.py'}",
    )

    # Task 5: Cost Management - Clean up raw files older than retention policy
    task_cleanup_staging = PythonOperator(
        task_id="cost_control_cleanup_staging",
        python_callable=purge_old_bronze_staging_files,
        op_kwargs={"days_retention": 7},
    )

    # Define orchestration dependency flow
    task_ingest_bronze >> task_bronze_to_silver >> task_quality_gate >> task_silver_to_gold >> task_cleanup_staging