from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor


with DAG(
    dag_id="silver_realestate_transform",
    schedule="@monthly",
    start_date=datetime(2023, 1, 1),
    catchup=True,
    tags=["realestate", "silver"],
) as dag:

    wait_for_bronze = ExternalTaskSensor(
        task_id="wait_for_bronze",
        external_dag_id="bronze_realestate_collect",
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=30,
        timeout=600,
    )

    run_silver_spark = BashOperator(
        task_id="run_silver_spark",
        bash_command="""
        spark-submit \
          --name silver_realestate_transform \
          --packages org.apache.hadoop:hadoop-aws:3.3.4 \
          /opt/airflow/q2_scripts/silver_spark.py \
          --yyyymm {{ logical_date.strftime('%Y%m') }} \
          --bucket realestate-parkdaeun
        """,
        execution_timeout=timedelta(minutes=20),
    )

    wait_for_bronze >> run_silver_spark