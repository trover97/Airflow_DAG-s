from pathlib import Path

from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.sensors.python_sensor import PythonSensor
from airflow import DAG

import datetime

dag = DAG(
    dag_id="supermarket_python_sensor",
    start_date=datetime.datetime(2020, 10, 20),
)


task1 = DummyOperator(
    task_id="task1",
    dag=dag,
)


def _wait_for_supermarket(supermarket_id):
    supermarket_path = Path("/data/" + supermarket_id)
    data_files = supermarket_path.glob("data-*.csv")
    success_file = supermarket_path / "_SUCCESS"
    return data_files and success_file.exists()


wait_for_supermarket_1 = PythonSensor(
    task_id="wait_for_supermarket_1",
    python_callable=_wait_for_supermarket,
    op_kwargs={"supermarket_id": "supermarket1"},
    dag=dag,
)

wait_for_supermarket_1 >> task1
