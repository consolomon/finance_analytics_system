from datetime import date, timedelta
from logging import Logger
from typing import Dict
from vertica_python import Connection

from py.workflow_settings import EtlSetting, get_setting, save_setting
from py.vertica import VerticaConnection, vertica_operator


class DdsLoader:

    def __init__(self, connection: VerticaConnection, wf_args: Dict, batch_date: date, log: Logger) -> None:

        self.connection = connection
        self.wf_key = wf_args['WF_KEY']
        self.last_loaded_date_key = wf_args['LAST_LOADED_DATE_KEY']
        self.path_to_check_script = wf_args['PATH_TO_CHECK_SCRIPT']
        self.path_to_etl_sctipt = wf_args['PATH_TO_ETL_SCRIPT']
        
        self.batch_date = batch_date
        self.log = log

    def load_items(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        try: 
            with self.connection.get_connection() as conn:
    
                # Прочитываем состояние загрузки
                # Если настройки еще нет, заводим ее.
                wf_setting = get_setting(conn, self.wf_key)
                if not wf_setting:
                    wf_setting = EtlSetting(
                        workflow_key=self.wf_key,
                        workflow_field_name=self.last_loaded_date_key,
                        workflow_field_value=self.batch_date
                    )
    
                # Вычитываем очередную пачку объектов.
                last_loaded_date = wf_setting.workflow_field_value
                last_staging_date = self.get_last_date(conn)
                if last_staging_date <= last_loaded_date:
                    self.log.info("Quitting.")
                    return
                
                # Сохраняем объекты в базу dwh.
                script_args = {
                    self.last_loaded_date_key: self.batch_date,
                }
                vertica_operator(self.log, 'insert', self.path_to_etl_sctipt, script_args, connection=conn)
    
                # Сохраняем прогресс.
                wf_setting.workflow_field_value = last_loaded_date + timedelta(days=1)
                save_setting(conn, wf_setting)
    
                self.log.info(f"Load finished successfully for: {self.wf_key}")
        except Exception as e:
            self.log.error(f"{self.wf_key} LOADER: error during load_items: {e}")
            raise

    def get_last_date(self, conn: Connection):
        try:
            last_date = vertica_operator(self.log, 'select', self.path_to_check_script, connection=conn)
            self.log.info(f'Check last_loaded_id: {last_date}')
            return last_date
        except Exception as e:
            self.log.error(f"{self.wf_key} LOADER: error during get_last_date: {e}")
            raise
