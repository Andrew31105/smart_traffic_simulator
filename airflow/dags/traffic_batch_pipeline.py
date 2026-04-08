from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "traffic_admin",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="traffic_batch_pipeline",
    default_args=default_args,
    schedule="@hourly",
    catchup=False,
    tags=["traffic", "batch", "spark"],
) as dag:
    run_spark_batch_job = BashOperator(
        task_id="run_spark_batch_job",
        bash_command="cd /opt/project && python src/batch/spark_batch_layer.py",
    )

    run_spark_batch_job
