import os
import datetime as dt
import zipfile

import requests
import pandas as pd
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    dag_id='covid_RU_BY_US_24hr',
    start_date=dt.datetime(2020, 10, 1),
    schedule_interval='@daily',
    catchup=False,
)


def _download_dataset():
    data_set_url = 'https://covid19.who.int/WHO-COVID-19-global-data.csv'
    response = requests.get(data_set_url)
    ds_name = 'COVID-19-GLOBAL-DATASET.csv'
    ds_file = open(ds_name, 'wb')
    ds_file.write(response.content)
    ds_file.close()


def get_df_country(country):
    df = pd.read_csv('COVID-19-GLOBAL-DATASET.csv')
    return df[df[' Country'] == country]


def _get_russia():
    rf_df = get_df_country('Russian Federation')
    rf_df.to_csv(path_or_buf='COVID-19-RUSSIA-DATASET.csv')


def _get_belarus():
    by_df = get_df_country('Belarus')
    by_df.to_csv(path_or_buf='COVID-19-BELARUS-DATASET.csv')


def _get_usa():
    us_df = get_df_country('United States of America')
    us_df.to_csv(path_or_buf='COVID-19-USA-DATASET.csv')


def _clear_dataset():
    countries = ['RUSSIA', 'BELARUS', 'USA']
    tmp_df = None

    for c in countries:
        tmp_df = pd.read_csv(f'COVID-19-{c}-DATASET.csv').drop(columns=[' Country', ' Country_code'])
        tmp_df.to_csv(f'COVID-19-{c}-DATASET.csv')


download_dataset = PythonOperator(
    task_id='download_dataset',
    python_callable=_download_dataset,
    dag=dag,
)

get_russia = PythonOperator(
    task_id='get_russia',
    python_callable=_get_russia,
    dag=dag,
)

get_belarus = PythonOperator(
    task_id='get_belarus',
    python_callable=_get_belarus,
    dag=dag,
)

get_usa = PythonOperator(
    task_id='get_usa',
    python_callable=_get_usa,
    dag=dag,
)

clear_dataset = PythonOperator(
    task_id='clear_dataset',
    python_callable=_clear_dataset,
    dag=dag,
)

download_dataset >> get_russia >> clear_dataset
download_dataset >> get_belarus >> clear_dataset
download_dataset >> get_usa >> clear_dataset
