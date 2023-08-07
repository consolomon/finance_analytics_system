-- H_CURRENCY

INSERT INTO KOSYAK1998YANDEXRU__DWH.h_currency (
    hk_currency_id,
    currency_id,
    load_dt,
    load_src
)
SELECT DISTINCT 
    hash(C.currency_code) AS hk_currency_id,
    C.currency_code AS currency_id,
    now() AS load_dt,
    's3' AS load_src
FROM KOSYAK1998YANDEXRU__STAGING.transactions AS T
FULL OUTER JOIN KOSYAK1998YANDEXRU__STAGING.currencies AS C
ON 
    T.currency_code = C.currency_code
WHERE hash(C.currency_code) NOT IN (
    SELECT hk_currency_id
    FROM KOSYAK1998YANDEXRU__DWH.h_currency
) AND C.date_update = '(:endpoint)';