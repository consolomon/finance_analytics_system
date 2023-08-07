from airflow import DAG
from airflow.operators.python import PythonOperator

import datetime
import logging

from py.dds_loader import DdsLoader
from py.vertica import VerticaConnection


log = logging.getLogger(__name__)
VERTICA_CONN_ID = 'VERTICA'
HUBS_WF_ARGS_KEYS = ["H_TRANSACTION", "H_CURRENCY", "H_ACCOUNT"]
LINKS_WF_ARGS_KEYS = ["L_TRANSACTION_ACCOUNT_FROM_TO", "L_TRANSACTION_CURRENCY"]
SATTELITES_WF_ARGS_KEYS = ["S_CURRENCY_DIV", "S_TRANSACTION_REQUISITES"]
CDM_WF_ARGS_KEYS =["GLOBAL_METRICS"]
WF_ARGS = {
    "H_TRANSACTION": {
        "WF_KEY": "H_TRANSACTION_STAGING_TO_DDS",
        "LAST_LOADED_DATE_KEY": "transaction_dt",
        "PATH_TO_CHECK_SCRIPT": "/src/sql/dds/transactions_check.sql",
        "PATH_TO_ETL_SCRIPT": "/src/sql/dds/h_transaction_dml.sql"
    },
    "H_CURRENCY": {
        "WF_KEY": "H_CURRENCY_STAGING_TO_DDS",
        "LAST_LOADED_DATE_KEY": "date_update",
        "PATH_TO_CHECK_SCRIPT": "/src/sql/dds/currencies_check.sql",
        "PATH_TO_ETL_SCRIPT": "/src/sql/dds/h_currency_dml.sql"
    },
    "H_ACCOUNT": {
        "WF_KEY": "H_ACCOUNT_STAGING_TO_DDS",
        "LAST_LOADED_DATE_KEY": "transaction_dt",
        "PATH_TO_CHECK_SCRIPT": "/src/sql/dds/transactions_check.sql",
        "PATH_TO_ETL_SCRIPT": "/src/sql/dds/h_account_dml.sql"
    },
    "L_TRANSACTION_ACCOUNT_FROM_TO": {
        "WF_KEY": "L_TRANSACTION_ACCOUNT_FROM_TO_STAGING_TO_DDS",
        "LAST_LOADED_DATE_KEY": "transaction_dt",
        "PATH_TO_CHECK_SCRIPT": "/src/sql/dds/transactions_check.sql",
        "PATH_TO_ETL_SCRIPT": "/src/sql/dds/l_transaction_account_from_to_dml.sql"
    },
    "L_TRANSACTION_CURRENCY": {
        "WF_KEY": "L_TRANSACTION_CURRENCY_STAGING_TO_DDS",
        "LAST_LOADED_DATE_KEY": "transaction_dt",
        "PATH_TO_CHECK_SCRIPT": "/src/sql/dds/transactions_check.sql",
        "PATH_TO_ETL_SCRIPT": "/src/sql/dds/l_transaction_currency_dml.sql"
    },
    "S_CURRENCY_DIV": {
        "WF_KEY": "S_CURRENCY_DIV_STAGING_TO_DDS",
        "LAST_LOADED_DATE_KEY": "date_update",
        "PATH_TO_CHECK_SCRIPT": "/src/sql/dds/currencies_check.sql",
        "PATH_TO_ETL_SCRIPT": "/src/sql/dds/s_currency_div_dml.sql"
    },
    "S_TRANSACTION_REQUISITES": {
        "WF_KEY": "S_TRANSACTION_REQUISITES_STAGING_TO_DDS",
        "LAST_LOADED_DATE_KEY": "transaction_dt",
        "PATH_TO_CHECK_SCRIPT": "/src/sql/dds/transactions_check.sql",
        "PATH_TO_ETL_SCRIPT": "/src/sql/dds/s_transaction_requisites_dml.sql"
    },
    "GLOBAL_METRICS": {
        "WF_KEY": "S_TRANSACTION_REQUISITES_STAGING_TO_DDS",
        "LAST_LOADED_DATE_KEY": "transaction_dt",
        "PATH_TO_CHECK_SCRIPT": "/src/sql/dds/transactions_check.sql",
        "PATH_TO_ETL_SCRIPT": "/src/sql/dds/s_transaction_requisites_dml.sql"
    },
    
    
}


def hubs_loader():
    execution_date = {{ds}}
    connection = VerticaConnection(VERTICA_CONN_ID)
    for h_wf_args_key in HUBS_WF_ARGS_KEYS:
        h_wf_args = WF_ARGS[h_wf_args_key]
        DdsLoader(connection, h_wf_args, execution_date, log)

def links_loader():
    execution_date = {{ds}}
    connection = VerticaConnection(VERTICA_CONN_ID)
    for l_wf_args_key in LINKS_WF_ARGS_KEYS:
        l_wf_args = WF_ARGS[l_wf_args_key]
        DdsLoader(connection, l_wf_args, execution_date, log)

def sattelites_loader():
    execution_date = {{ds}}
    connection = VerticaConnection(VERTICA_CONN_ID)
    for s_wf_args_key in SATTELITES_WF_ARGS_KEYS:
        s_wf_args = WF_ARGS[s_wf_args_key]
        DdsLoader(connection, s_wf_args, execution_date, log)

def cdm_loader():
    execution_date = {{ds}}
    connection = VerticaConnection(VERTICA_CONN_ID)
    for cdm_wf_args_key in CDM_WF_ARGS_KEYS:
        cdm_wf_args = WF_ARGS[cdm_wf_args_key]
        DdsLoader(connection, cdm_wf_args, execution_date, log)      

with DAG(
    dag_id='staging_to_dds_cdm_dag',
    catchup=True,
    start_date=datetime.date(2022, 10, 1),
    end_date=datetime.date(2022, 10, 31),
    tags=['final_project', 'staging', 'dds']
) as dag:

    hubs_etl = PythonOperator(
        task_id='hubs_etl',
        python_callable=hubs_loader
    )
    
    links_etl = PythonOperator(
        task_id='links_etl',
        python_callable=links_loader
    )
    
    sattelites_etl = PythonOperator(
        task_id='sattelites_etl',
        python_callable=sattelites_loader
    )

    hubs_etl >> links_etl >> sattelites_etl
    
    