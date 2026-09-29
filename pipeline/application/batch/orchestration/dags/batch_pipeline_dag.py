"""Airflow DAG for batch transaction processing pipeline.

This DAG orchestrates the batch processing of transaction data:
- Extracts data from S3/MinIO
- Validates transaction records
- Performs ML classification predictions
- Loads results into PostgreSQL database

Schedule: Daily at 2 AM UTC
"""

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.bases.hook import BaseHook

logger = logging.getLogger(__name__)


class DynamicDockerOperator(DockerOperator):
    """Custom DockerOperator that pulls environment variables from XCom."""

    def __init__(self, xcom_task_id: str, xcom_key: str = "return_value", **kwargs):
        """
        Initialize with XCom task ID to pull environment from.

        Args:
            xcom_task_id: Task ID to pull XCom value from
            xcom_key: XCom key to use (default: 'return_value')
        """
        self.xcom_task_id = xcom_task_id
        self.xcom_key = xcom_key
        super().__init__(**kwargs)

    def execute(self, context):
        """Pull environment from XCom before executing."""
        # Pull environment variables from XCom
        ti = context.get("ti")
        if ti:
            env_vars = ti.xcom_pull(task_ids=self.xcom_task_id, key=self.xcom_key)
            # Update the environment parameter
            if env_vars:
                self.environment = env_vars

        # Call parent execute
        return super().execute(context)


# Default arguments for the DAG
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# Define the DAG
with DAG(
    dag_id="batch_transaction_pipeline",
    default_args=default_args,
    description="Batch processing pipeline for transaction classification on transaction data",
    schedule="*/2 * * * *",  # Every 2 minutes
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["batch", "ml", "transactions"],
) as dag:
    # Batch processing configuration (adjust as needed)
    BATCH_CONFIG = {
        "ROW_BATCH_SIZE": "5000",  # Number of rows to process at once from CSV
        "API_BATCH_SIZE": "100",  # Number of records per ML API request
        "API_MAX_WORKERS": "5",  # Concurrent API request workers
        "DB_ROW_BATCH_SIZE": "1000",  # Number of rows per database insert
    }

    # Get connections and secrets securely
    def get_environment_vars(**context):
        """Retrieve secrets from Airflow Connections and add batch configuration."""
        # Get Airflow run_id for lineage tracking
        run_id = context["run_id"]

        # Get PostgreSQL connection
        postgres_conn = BaseHook.get_connection("postgres_transactions")
        database_url = postgres_conn.get_uri()

        # Fix SQLAlchemy 2.x compatibility: replace postgres:// with postgresql://
        if database_url.startswith("postgres://"):
            database_url = database_url.replace("postgres://", "postgresql://", 1)

        # Get MinIO/S3 connection
        minio_conn = BaseHook.get_connection("minio_s3")
        endpoint_url = f"http://{minio_conn.host}:{minio_conn.port}"
        minio_key = minio_conn.login
        minio_secret = minio_conn.password

        # Get ML API connection
        ml_api_conn = BaseHook.get_connection("ml_api")
        ml_api_url = f"http://{ml_api_conn.host}:{ml_api_conn.port}"

        # Combine secrets with batch configuration and lineage info
        env_vars = {
            "DATABASE_URL": database_url,
            "ENDPOINT_URL": endpoint_url,
            "KEY": minio_key,
            "SECRET": minio_secret,
            "ML_API_URL": ml_api_url,
            "BATCH_RUN_ID": run_id,  # For lineage tracking
        }
        env_vars.update(BATCH_CONFIG)

        # Store in XCom for next task
        return env_vars

    get_env_task = PythonOperator(
        task_id="get_environment_vars",
        python_callable=get_environment_vars,
    )

    # Task: Run batch processing using custom DockerOperator
    run_batch_processing = DynamicDockerOperator(
        task_id="run_batch_processing",
        image="batch-processor:latest",
        api_version="auto",
        auto_remove="success",
        network_mode=os.getenv("PIPELINE_DOCKER_NETWORK", "transaction-enrichment-network"),
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        xcom_task_id="get_environment_vars",
    )

    # Task: Log completion
    def log_completion(**context):
        """Log pipeline completion with execution metadata."""
        # Airflow 3.x uses 'logical_date' instead of 'execution_date'
        logical_date = context.get("logical_date") or context.get("data_interval_start")
        run_id = context["run_id"]
        logger.info(f"Batch pipeline completed successfully for {logical_date}")
        logger.info(f"Run ID: {run_id}")
        return "success"

    log_completion_task = PythonOperator(
        task_id="log_completion",
        python_callable=log_completion,
    )

    # Define task dependencies
    get_env_task >> run_batch_processing >> log_completion_task  # type: ignore[expression-value]
