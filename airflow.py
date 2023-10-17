from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 5, 10),
    "email": ["yashwanth.sonub@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=60),
    "catchup": False
}

dag = DAG(
    "dockerhub",
    description="dockerhub loader",
    schedule_interval="0 10 * * *",
    default_args=default_args,
    catchup=False,
    tags=["tdap-db01"],
)

all = BashOperator(
        task_id='dockerhub_loader',
        bash_command='python3 /home/bduser/dockerhub/dockerhub_loader.py -es Prod',
        dag=dag
    )

[all]
