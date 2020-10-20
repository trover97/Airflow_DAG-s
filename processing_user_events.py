import pandas as pd
from datetime import datetime

import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    dag_id="user_events",
    start_date=datetime(2015, 6, 1),
    schedule_interval='@daily',
)

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command="curl -o data/events/{{ds}}.json"
                 "https://localhost:5000/events?"
                 "start_date={{ds}}"
                 "&end_date={{next_ds}}",
    dag=dag,
)


def _calculate_stats(**context):
    input_path = context["templates_dict"]["input_path"]
    output_path = context["templates_dict"]["output_path"]

    events = pd.read_json(input_path)
    stats = events.groupby(["date", "user"]).size().reset_index()
    stats.to_csv(output_path, index=False)

    send_stats(stats, email="user@example.com")


calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    op_kwargs={
        "input_path": "data/events/{{ds}}.json",
        "output_path": "data/stats/{{ds}}.csv",
    },
    provide_context=True,
    dag=dag,
)


def email_stats(stats, email):
    pass


def _send_stats(email, **context):
    stats = pd.read_csv(context["templates_dict"]["stats_path"])
    email_stats(stats, email=email)


send_stats = PythonOperator(
    task_id="send_stats",
    python_callable=_send_stats,
    op_kwargs={"email": "user@example.com"},
    templates_dict={"stats_path": "data/stats/{{ds}}.csv"},
    provide_context=True,
    dag=dag,
)

fetch_events >> calculate_stats
calculate_stats >> send_stats
