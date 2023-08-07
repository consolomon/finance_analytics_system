-- S_TRANSACTION_REQUISITES

INSERT INTO KOSYAK1998YANDEXRU__DWH.s_transaction_requisites (
    hk_transaction_id,
    "status",
    amount,
    transaction_dt,
    load_dt,
    load_src
)
SELECT 
    ht.hk_transaction_id,
    T."status",
    T.amount,
    T.transaction_dt,
    now() AS load_dt,
    's3' AS load_src
FROM KOSYAK1998YANDEXRU__DWH.h_transaction AS ht
LEFT JOIN KOSYAK1998YANDEXRU__STAGING.transactions AS T
ON ht.operation_id = T.operation_id
WHERE 
    ht.hk_transaction_id NOT IN (
    SELECT hk_transaction_id
    FROM KOSYAK1998YANDEXRU__DWH.s_transaction_requisites
)
AND T."status" = 'done'
AND T.transaction_dt = '(:endpoint)'
GROUP BY ht.hk_transaction_id, T."status", T.amount, T.transaction_dt; 