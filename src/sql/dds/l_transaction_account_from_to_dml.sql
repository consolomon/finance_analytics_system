-- L_TRANSACTIOM_ACCOUNT_FROM_TO

INSERT INTO KOSYAK1998YANDEXRU__DWH.l_transaction_account_from_to (
    hk_l_transaction_account_from_to,
    hk_transaction_id,
    hk_account_id_from,
    hk_account_id_to,
    load_dt,
    load_src
)
SELECT DISTINCT 
    hash(ht.hk_transaction_id, haf.hk_account_id, hat.hk_account_id) AS hk_l_transaction_account_from_to,
    ht.hk_transaction_id,
    haf.hk_account_id AS hk_account_id_from,
    hat.hk_account_id AS hk_account_id_to,
    now() AS load_dt,
    's3' AS load_src
FROM KOSYAK1998YANDEXRU__STAGING.transactions AS T
LEFT JOIN KOSYAK1998YANDEXRU__DWH.h_transaction AS ht
ON
    T.operation_id = ht.operation_id
LEFT JOIN KOSYAK1998YANDEXRU__DWH.h_account AS haf
ON
    T.account_number_from = haf.account_id
LEFT JOIN KOSYAK1998YANDEXRU__DWH.h_account AS hat
ON
    T.account_number_to = hat.account_id
WHERE hash(ht.hk_transaction_id, haf.hk_account_id, hat.hk_account_id) NOT IN (
    SELECT hk_l_transaction_account_from_to
    FROM KOSYAK1998YANDEXRU__DWH.l_transaction_account_from_to
) AND T.account_number_from >= 0 AND T.account_number_to >= 0
AND T.transaction_dt = '(:endpoint)';