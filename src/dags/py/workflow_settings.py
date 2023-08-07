from vertica_python import Connection
from typing import Dict, Optional
from datetime import date


class EtlSetting:
    def __init__(self, id: int, workflow_key: str, workflow_field_name: str, workflow_field_value: date) -> None:
        self.workflow_key = workflow_key
        self.workflow_field_name = workflow_field_name,
        self.workflow_field_value = workflow_field_value


def get_setting(conn: Connection, etl_key: str) -> Optional[EtlSetting]:
        with conn.cursor() as cur:
            cur.execute(
                """
                    SELECT
                        workflow_key,
                        workflow_field_name,
                        workflow_field_value
                    FROM KOSYAK1998YANDEXRU__DWH.srv_wf_settings
                    WHERE workflow_key = %(etl_key)s;
                """,
                {"etl_key": etl_key}
            )
            obj = cur.fetchone()
            if obj is not None:
                return EtlSetting(obj['workflow_key'], obj['workflow_field_name'], obj['workflow_field_value'])
            else:
                return None

def save_setting(conn: Connection, etl_setting: EtlSetting) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO KOSYAK1998YANDEXRU__DWH.srv_wf_settings(workflow_key, workflow_settings)
                    VALUES (%(etl_key)s, %(workflow_field_name)s, '%(workflow_field_value)s')
                    ON CONFLICT (workflow_key) DO UPDATE
                    SET workflow_settings = EXCLUDED.workflow_settings;
                """,
                {
                    "etl_key": etl_setting.workflow_key,
                    "workflow_field_name": etl_setting.workflow_field_name,
                    "workflow_field_value": etl_setting.workflow_field_value
                }
            )