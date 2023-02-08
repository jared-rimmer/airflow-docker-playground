from datetime import timedelta
from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.utils.dates import days_ago
from docker.types import Mount 
import os


default_args = {
    "owner": "jared-rimmer",
    "depends_on_past": False,
    "email": ["jared-rimmer@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "dbt",
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
)


dbt_run = DockerOperator(
    api_version="auto",
    docker_url="tcp://docker-proxy:2375",
    command="run",
    mounts=[
        Mount(
            source=os.getenv('DBT_SOURCE'), 
            target='/usr/app', 
            type='bind'
        ),
        Mount(
            source=os.getenv('DBT_PROFILE_YML'), 
            target='/root/.dbt/profiles.yml', 
            type='bind'
        ),
    ],
    mount_tmp_dir=False,
    image="ghcr.io/dbt-labs/dbt-postgres:1.4.1",
    network_mode="bridge",
    task_id="dbt_run",
    dag=dag,
)

dbt_run 