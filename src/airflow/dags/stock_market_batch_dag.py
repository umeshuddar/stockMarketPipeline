from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="stock_market_batch_pipeline",
    default_args=default_args,
    description="Stock Market Batch Processing Pipeline",
    schedule_interval="@daily",
    catchup=False,
    tags=["stock-market", "spark", "minio", "parquet"],
) as dag:

    spark_batch_process = DockerOperator(
        task_id="spark_batch_process",

        image="apache/spark:3.5.0",

        api_version="auto",

        auto_remove=True,

        docker_url="unix://var/run/docker.sock",

        network_mode="stockmarketpipeline_default",

        mount_tmp_dir=False,

        mounts=[
            Mount(
                source=r"D:\workspace\StockMarketPipeline\src\processing",
                target="/opt/spark-apps",
                type="bind",
            )
        ],

        command=(
            "/opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "--conf spark.jars.ivy=/tmp/.ivy2 "
            "--packages "
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262 "
            "/opt/spark-apps/spark_batch_processor.py "
            "{{ ds }}"
        ),
    )