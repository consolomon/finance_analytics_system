from airflow import DAG
from airflow.operators.python import PythonOperator

import datetime
import logging

from py.s3_loader import S3Loader
from py.s3_loader import s3_load_file
from py.vertica import VerticaConnection

"""
AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"

PATH_TO_SCRIPT = "/src/sql/dml/origin_to_staging.sql"
"""
VERTICA_CONN_ID = 'VERTICA'


# # # SQL settings # # #
PATH_TO_SCRIPT = '/src/sql/dml/origin_to_staging.sql'
SQL_PARAMETERS = {
    "transactions": "operation_id, account_number_from, account_number_to, currency_code, country, status, transaction_type, amount, transaction_dt",
    "currencies": "date_update, currency_code, currency_code_with, currency_code_div",
}

log = logging.getLogger(__name__)


def staging_currencies_etl():
    conn_info = VerticaConnection(VERTICA_CONN_ID).get_conn_info()
    s3_loader = S3Loader(PATH_TO_SCRIPT, conn_info, SQL_PARAMETERS)
    s3_load_file(s3_loader, 'currencies_history', 'currencies', log)

def staging_transactions_etl():
    conn_info = VerticaConnection(VERTICA_CONN_ID).get_conn_info()
    s3_loader = S3Loader(PATH_TO_SCRIPT, conn_info, SQL_PARAMETERS)
    for i in range(1, 11, 1):
        s3_load_file(s3_loader, f'transactions_batch_{i}', 'transactions', log)
    

with DAG(
    dag_id='origin_to_staging_dag',
    schedule_interval='0/30 * * * *',
    start_date=datetime.datetime.today() - datetime.timedelta(hours=2),
    catchup=False,
    dagrun_timeout=datetime.timedelta(seconds=60),
    tags=['final_project', 'origin', 'staging']
) as dag:

    upload_from_origin_currencies = PythonOperator(
        task_id='upload_from_origin_currencies',
        python_callable=staging_currencies_etl
    )

    upload_from_origin_transactions = PythonOperator(
        task_id='upload_from_origin_transactions',
        python_callable=staging_transactions_etl
    )
    
    upload_from_origin_currencies >> upload_from_origin_transactions