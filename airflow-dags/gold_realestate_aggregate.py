from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor


with DAG(
    dag_id="gold_realestate_aggregate",
    schedule="@monthly",
    start_date=datetime(2023, 1, 1),
    catchup=True,
    tags=["realestate", "gold"],
) as dag:

    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_silver",
        external_dag_id="silver_realestate_transform",
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=30,
        timeout=600,
    )

    run_gold_spark = BashOperator(
        task_id="run_gold_spark",
        bash_command="""
        spark-submit \
          --name gold_realestate_aggregate \
          --packages org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.4 \
          /opt/airflow/q3_scripts/gold_spark_sql.py \
          --bucket realestate-parkdaeun \
          --postgres-url jdbc:postgresql://postgres:5432/airflow \
          --postgres-user "$POSTGRES_USER" \
          --postgres-password "$POSTGRES_PASSWORD"
        """,
        execution_timeout=timedelta(minutes=20),
    )

    wait_for_silver >> run_gold_spark