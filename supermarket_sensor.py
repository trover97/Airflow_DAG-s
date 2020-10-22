from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
import airflow
import datetime

dag = DAG(
    dag_id="supermarket_sensor",
    start_date=datetime.datetime(2020, 10, 20),
)

task1 = DummyOperator(
    task_id="task1",
    dag=dag,
)

task2 = DummyOperator(
    task_id="task2",
    dag=dag,
)

wait_for_supermarket_1 = FileSensor(
    task_id="wait_for_supermarket_1",
    filepath="/data/supermarket1/data.csv",
    dag=dag,
)

task1 >> wait_for_supermarket_1 >> task2
