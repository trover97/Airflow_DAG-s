import datetime as dt
from datetime import timedelta
import yahoo_fin.stock_info as si

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


dag = DAG(
    dag_id="show_nflx_price_5min",
    start_date=days_ago(1),
    schedule_interval=timedelta(minutes=5)
)

price = 0


def _get_price():
    global price
    price = si.get_live_price('nflx')


def _print_time():
    print(f"price = {price} ", end='')
    print(f"time = {dt.datetime.now()}")


get_price = PythonOperator(
    task_id='get_price',
    python_callable=_get_price,
    dag=dag,
)

print_price_time = PythonOperator(
    task_id='print_time',
    python_callable=_print_time,
    dag=dag,
)

get_price >> print_price_time
