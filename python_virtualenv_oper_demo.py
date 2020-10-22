import pandas as pd
import numpy as np
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import PythonVirtualenvOperator

dag = DAG(
    dag_id="python_virtualenv_oper_demo",
    start_date=datetime(2020, 10, 19),
    schedule_interval='@daily',
)


def _create_random_dataset():
    arr = np.random.normal(0, 1, 500)
    np.savetxt("random_dataset.csv", arr)


create_random_dataset = PythonOperator(
    task_id='create_random_dataset',
    python_callable=_create_random_dataset,
    dag=dag,
)


def python_code():
    from faker import Faker
    from bashplotlib.histogram import plot_hist
    import numpy as np

    fake = Faker()
    arr = np.fromfile("random_dataset.csv")
    print(f'{fake.name()} has {fake.random_number()}$ in wallet\n')
    plot_hist(arr, bincount=5)


task1 = PythonVirtualenvOperator(
    task_id='python-virtual-env-demo',
    python_callable=python_code,
    requirements=['bashplotlib', "faker"],
    python_version='3',
    dag=dag)

create_random_dataset >> task1
