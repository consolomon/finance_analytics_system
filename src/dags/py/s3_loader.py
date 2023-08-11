import boto3

from pathlib import Path
from logging import Logger
from typing import Dict
from py.vertica import vertica_operator

from airflow.models import Variable

class S3Loader:

    def __init__(self, path_to_script: str, conn_info: Dict, sql_parameters: Dict):

        self.aws_access_key_id = Variable.get('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = Variable.get('AWS_SECRET_ACCESS_KEY')
        self.aws_secret_access_key = Variable.get('')
        self.path_to_script = path_to_script
        self.conn_info = conn_info
        self.sql_parameters = sql_parameters


def s3_load_file(s3_loader: S3Loader, key: str, table: str, log: Logger) -> None:
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net/',
        aws_access_key_id=s3_loader.aws_access_key_id,
        aws_secret_access_key=s3_loader.aws_secret_access_key
    )
    log.info("S3 connection succses")
    s3_client.download_file(
        Bucket='final-project',
        Key=f'{key}.csv',
        Filename=f'/data/{key}.csv'
    )
    log.info(f"S3: {key} file downloading succsesful")
    script = Path(s3_loader.path_to_script).read_text()
    log.info("Prepared script to execute:")
    log.info(script)  
    if s3_loader.sql_parameters.keys().__contains__(table):
        script_args = {
            'sql_key': key,
            'sql_table': table,
            'sql_parameters': s3_loader.sql_parameters[table]
        }  
        vertica_operator(log, 'insert', s3_loader.path_to_script, script_args, s3_loader.conn_info)
    else:
        log.error("This source is unacceptable: add this sourse into the SQL_PARAMETERS dict")
        raise KeyError("Key not found in SQL_PARAMETERS dict")   
    