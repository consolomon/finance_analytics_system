-- S_CURRENCY_DIV

INSERT INTO KOSYAK1998YANDEXRU__DWH.s_currency_div (
    hk_currency_id,
    hk_currency_id_with,
    date_update,
    currency_div,
    load_dt,
    load_src
)
SELECT
    hc.hk_currency_id,
    hcw.hk_currency_id AS hk_currency_id_with,
    C.date_update,
    C.currency_code_div AS currency_div,
    now() AS load_dt,
    's3' AS load_src
FROM KOSYAK1998YANDEXRU__STAGING.currencies AS C
LEFT JOIN KOSYAK1998YANDEXRU__DWH.h_currency AS hc
ON 
    C.currency_code = hc.currency_id
LEFT JOIN KOSYAK1998YANDEXRU__DWH.h_currency AS hcw
ON
    C.currency_code_with = hcw.currency_id
WHERE hc.hk_currency_id NOT IN (
    SELECT hk_currency_id
    FROM KOSYAK1998YANDEXRU__DWH.s_currency_div
) AND
    hcw.hk_currency_id NOT IN (
    SELECT hk_currency_id_with
    FROM KOSYAK1998YANDEXRU__DWH.s_currency_div
) AND C.date_update = '(:endpoint)';