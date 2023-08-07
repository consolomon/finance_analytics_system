-- L_TRANSACTION_CURRENCY

INSERT INTO KOSYAK1998YANDEXRU__DWH.l_transaction_currency (
    hk_l_transaction_currency,
    hk_transaction_id,
    hk_currency_id,
    load_dt,
    load_src 
)
SELECT
    hash(ht.hk_transaction_id, hc.hk_currency_id) AS hk_l_transaction_currency,
    ht.hk_transaction_id,
    hc.hk_currency_id,
    now() AS load_dt,
    's3' AS load_src
FROM KOSYAK1998YANDEXRU__STAGING.transactions AS T
LEFT JOIN KOSYAK1998YANDEXRU__DWH.h_transaction AS ht
ON 
    T.operation_id = ht.operation_id
LEFT JOIN KOSYAK1998YANDEXRU__DWH.h_currency AS hc
ON 
    T.currency_code = hc.currency_id
WHERE hash(ht.hk_transaction_id, hc.hk_currency_id) NOT IN (
    SELECT hk_l_transaction_currency
    FROM KOSYAK1998YANDEXRU__DWH.l_transaction_currency
) AND T.transaction_dt = '(:endpoint)';