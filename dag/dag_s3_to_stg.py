import subprocess
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
from datetime import datetime, timedelta
import boto3
from airflow import DAG
import logging
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os

package_name = 'pyarrow'
subprocess.check_call(['pip', 'install', package_name])


def get_files():

    logging.info(f'Creating SQL-Alchemy engine')

    engine = create_engine('postgresql://jovyan:jovyan@158.160.62.179:5432/postgres')

    logging.info(f'SQL-Alchemy engine DONE')

    with engine.connect() as conn:
            conn.execute('TRUNCATE TABLE stg.events')

    logging.info(f'Table events SUCCESFULLY Trunated')

    url = 'https://storage.yandexcloud.net/hackathon/events-2022-Sep-30-2134.parquet'
    df = pd.read_parquet(url, 'pyarrow')

    engine = create_engine('postgresql://jovyan:jovyan@158.160.62.179:5432/postgres')

    df.to_sql('events', engine, schema='stg', if_exists='replace', index=False)

    logging.info(f'DATA SUCCESFULLY UPLOADED')




default_args = {
    'owner': 'airflow_admin',
    'start_date': datetime(2023, 6, 3)
}

with DAG(
    'project_5_loading_group_log',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    catchup=False,
    tags=['hackaton'],
    is_paused_upon_creation=False

) as dag:

    get_file_from_s3 = PythonOperator(
        task_id='s3_to_stg',   
        python_callable=get_files,
        dag=dag
    )

    get_file_from_s3