INSERT INTO KOSYAK1998YANDEXRU__DWH.global_metrics (
    date_update,
    currency_from,
    amount_total,
    cnt_transactions,
    avg_transactions_per_account,
    cnt_accounts_make_transactions
)
SELECT
    A.date_update,
    A.currency_from,
    CASE 
        WHEN A.currency_from = 420 THEN A.sum_transactions
        ELSE ROUND(A.sum_transactions * CD.currency_div)
    END amount_total,
    A.cnt_transactions,
    ROUND(A.cnt_transactions / A.cnt_accounts_make_transactions) AS avg_transactions_per_account,
    A.cnt_accounts_make_transactions
FROM (
    SELECT
        str.transaction_dt::Date as date_update,
        hc.currency_id AS currency_from,
        SUM(ABS(str.amount)) AS sum_transactions,
        COUNT(ht.hk_transaction_id) AS cnt_transactions,
        COUNT(DISTINCT lta.hk_account_id_from) as cnt_accounts_make_transactions
    FROM KOSYAK1998YANDEXRU__DWH.h_transaction AS ht
    LEFT JOIN KOSYAK1998YANDEXRU__DWH.l_transaction_currency AS ltc
    ON ht.hk_transaction_id = ltc.hk_transaction_id
    LEFT JOIN KOSYAK1998YANDEXRU__DWH.h_currency AS hc
    ON ltc.hk_currency_id = hc.hk_currency_id
    LEFT JOIN KOSYAK1998YANDEXRU__DWH.s_transaction_requisites AS str
    ON ht.hk_transaction_id = str.hk_transaction_id
    LEFT JOIN KOSYAK1998YANDEXRU__DWH.l_transaction_account_from_to AS lta
    ON ht.hk_transaction_id = lta.hk_transaction_id
    WHERE str.transaction_dt::Date = '(:endpoint)'
    GROUP BY str.transaction_dt::Date, hc.currency_id
    AND 
) AS A
LEFT JOIN (
    SELECT 
        scd.date_update::Date as date_update,
        hc.currency_id AS currency_from,
        scd.currency_div
    FROM  KOSYAK1998YANDEXRU__DWH.h_currency AS hc
    LEFT JOIN KOSYAK1998YANDEXRU__DWH.s_currency_div AS scd
    ON hc.hk_currency_id = scd.hk_currency_id_with
    WHERE scd.hk_currency_id = 2856671230678627208
    AND scd.date_update::Date = '(:endpoint)'
) AS CD
ON A.date_update = CD.date_update AND A.currency_from = CD.currency_from
WHERE A.date_update IS NOT NULL;

