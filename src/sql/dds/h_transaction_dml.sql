--H_TRANSACTION

INSERT INTO KOSYAK1998YANDEXRU__DWH.h_transaction (
    hk_transaction_id,
    operation_id,
    load_dt,
    load_src
)
SELECT DISTINCT
    hash(T.operation_id) AS hk_transaction_id,
    T.operation_id,
    now() AS load_dt,
    's3' AS load_src
FROM KOSYAK1998YANDEXRU__STAGING.transactions AS T
WHERE 
    hash(T.operation_id) NOT IN (
        SELECT hk_transaction_id
        FROM KOSYAK1998YANDEXRU__DWH.h_transaction
) AND (T.transaction_dt = '(:endpoint)');