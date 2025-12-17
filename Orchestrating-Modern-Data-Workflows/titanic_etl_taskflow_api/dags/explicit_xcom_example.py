from airflow.decorators import dag, task
from airflow.models import XCom
from datetime import datetime

@dag(schedule=None, start_date=datetime(2023, 1, 1), catchup=False)
def explicit_xcom_example():

    @task
    def push_task(ti=None):
        ti.xcom_push(key="my_key", value="Custom XCom Data")

    @task
    def pull_task(ti=None):
        data = ti.xcom_pull(task_ids="push_task", key="my_key")
        print(f"Explicitly pulled: {data}")

    push_task() >> pull_task()

explicit_xcom_example()