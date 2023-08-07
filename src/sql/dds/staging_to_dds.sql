-- H_TRANSACTION

TRUNCATE TABLE KOSYAK1998YANDEXRU__DWH.h_transaction;

INSERT INTO KOSYAK1998YANDEXRU__DWH.h_transaction (
    hk_transaction_id,
    operation_id,
    load_dt,
    load_src
)
SELECT
    hash(T.operation_id) AS hk_transaction_id,
    T.operation_id,
    now() AS load_dt,
    's3' AS load_src
FROM KOSYAK1998YANDEXRU__STAGING.transactions AS T
ORDER BY T.operation_id ASC 
WHERE 
    hash(T.operation_id) NOT IN (
        SELECT hk_transaction_id
        FROM KOSYAK1998YANDEXRU__DWH.h_transaction
    AND T.operation_id > %(endpoint)s
LIMIT %(batch_limit)s
);

-- H_ACCOUNT

TRUNCATE TABLE KOSYAK1998YANDEXRU__DWH.h_account;

INSERT INTO KOSYAK1998YANDEXRU__DWH.h_account (
    hk_account_id,
    account_id,
    load_dt,
    load_src
)
SELECT
    hash(A.account_id) AS hk_account_id,
    A.account_number_from,
    now() AS load_dt,
    's3' AS load_src
FROM (
    SELECT
        account_number_from as account_id
    FROM KOSYAK1998YANDEXRU__STAGING.transactions
    UNION
    SELECT
        account_number_to as account_id
    FROM KOSYAK1998YANDEXRU__STAGING.transactions AS
) AS A
WHERE hash(A.operation_id) NOT IN (
    SELECT hk_transaction_id
    FROM KOSYAK1998YANDEXRU__DWH.h_transaction
);

-- H_CURRENCY

TRUNCATE TABLE KOSYAK1998YANDEXRU__DWH.h_currency;

INSERT INTO KOSYAK1998YANDEXRU__DWH.h_currency (
    hk_currency_id,
    currency_id,
    load_dt,
    load_src
)
SELECT
    hash(C.currency_code) AS hk_currency_id,
    C.currency_code AS currency_id,
    now() AS load_dt,
    's3' AS load_src
FROM KOSYAK1998YANDEXRU__STAGING.transactions AS T
OUTER JOIN KOSYAK1998YANDEXRU__STAGING.currencies AS C
ON 
    T.currency_code = C.currency_code
WHERE hash(C.currency_code) NOT IN (
    SELECT hk_currency_id
    FROM KOSYAK1998YANDEXRU__DWH.h_currency
);

-- L_TRANSACTIOM_ACCOUNT_FROM_TO

TRUNCATE TABLE KOSYAK1998YANDEXRU__DWH.l_transaction_account_from_to;

INSERT INTO KOSYAK1998YANDEXRU__DWH.l_transaction_account_from_to (
    hk_l_transaction_account_from_to,
    hk_transaction_id,
    hk_account_id_from,
    hk_account_id_to,
    load_dt,
    load_src
)
SELECT
    hash(ht.hk_transaction_id, haf.hk_account_id, hat.hk_account_id) AS hk_l_transaction_account_from_to,
    ht.hk_transaction_id,
    haf.hk_account_id AS hk_account_id_from,
    hat.hk_account_id AS hk_account_id_to,
    now() AS load_dt,
    's3' AS load_src
FROM KOSYAK1998YANDEXRU__STAGING.transactions AS T
LEFT JOIN KOSYAK1998YANDEXRU__DWH.h_transactions AS ht
ON
    T.operation_id = ht.operation_id
LEFT JOIN KOSYAK1998YANDEXRU__DWH.h_account AS haf
ON
    T.account_number_from = haf.account_id
LEFT JOIN KOSYAK1998YANDEXRU__DWH.h_account AS hat
ON
    T.account_number_from = hat.account_id   
WHERE hash(ht.hk_transaction_id, haf.hk_account_id, hat.hk_account_id) NOT IN (
    SELECT hk_l_transaction_account_from_to
    FROM KOSYAK1998YANDEXRU__DWH.l_transaction_account_from_to
);

-- L_TRANSACTION_CURRENCY

TRUNCATE TABLE KOSYAK1998YANDEXRU__DWH.l_transaction_currency;

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
LEFT JOIN KOSYAK1998YANDEXRU__DWH.h_transactions AS ht
ON 
    T.operation_id = ht.operation_id
LEFT JOIN KOSYAK1998YANDEXRU__DWH.h_currency AS hc
ON 
    T.currency_code = hc.currency_id
WHERE hash(ht.hk_transaction_id, hc.hk_currency_id) NOT IN (
    SELECT hk_l_transaction_currency
    FROM KOSYAK1998YANDEXRU__DWH.l_transaction_currency
);