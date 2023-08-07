-- H_ACCOUNT

INSERT INTO KOSYAK1998YANDEXRU__DWH.h_account (
    hk_account_id,
    account_id,
    load_dt,
    load_src
)
SELECT DISTINCT
    hash(A.account_id) AS hk_account_id,
    A.account_id,
    now() AS load_dt,
    's3' AS load_src
FROM (
    SELECT
        account_number_from as account_id
    FROM KOSYAK1998YANDEXRU__STAGING.transactions
    WHERE operation_dt = '(:endpoint)'
    UNION
    SELECT
        account_number_to as account_id
    FROM KOSYAK1998YANDEXRU__STAGING.transactions
    WHERE operation_dt = '(:endpoint)'
) AS A
WHERE hash(A.account_id) NOT IN (
    SELECT hk_account_id
    FROM KOSYAK1998YANDEXRU__DWH.h_account
) AND A.account_id >= 0;