DROP TABLE IF EXISTS KOSYAK1998YANDEXRU__STAGING.transactions;

CREATE TABLE IF NOT EXISTS KOSYAK1998YANDEXRU__STAGING.transactions (
    operation_id uuid NOT NULL,
    account_number_from integer NOT NULL,
    account_number_to integer NOT NULL,
    currency_code integer NOT NULL,
    country varchar(30) NOT NULL,
    "status" varchar(20) NOT NULL,
    transaction_type varchar(30) NOT NULL,
    amount integer NOT NULL,
    transaction_dt timestamp(3) NOT NULL,
    CONSTRAINT operation_id_pkey PRIMARY KEY (operation_id) 
)
ORDER BY transaction_dt
SEGMENTED BY HASH(transaction_dt::Date, operation_id) ALL NODES
PARTITION BY transaction_dt::Date
GROUP BY CALENDAR_HIERARCHY_DAY(transactions.transaction_dt::Date, 3, 2);

CREATE PROJECTION IF NOT EXISTS KOSYAK1998YANDEXRU__STAGING.transactions_by_date
(
 operation_id,
 account_number_from,
 account_number_to,
 currency_code,
 country,
 status,
 transaction_type,
 amount,
 transaction_dt
)
AS
    SELECT transactions.operation_id,
        transactions.account_number_from,
        transactions.account_number_to,
        transactions.currency_code,
        transactions.country,
        transactions.status,
        transactions.transaction_type,
        transactions.amount,
        transactions.transaction_dt
    FROM KOSYAK1998YANDEXRU__STAGING.transactions
SEGMENTED BY hash((transactions.transaction_dt)::Date) ALL NODES KSAFE 1;


DROP TABLE IF EXISTS KOSYAK1998YANDEXRU__STAGING.currencies;

CREATE TABLE IF NOT EXISTS KOSYAK1998YANDEXRU__STAGING.currencies (
    date_update timestamp(3) NOT NULL,
    currency_code integer NOT NULL,
    currency_code_with integer NOT NULL,
    currency_code_div NUMERIC(12, 3) NOT NULL,
    CONSTRAINT operation_id_pkey PRIMARY KEY (date_update, currency_code, currency_code_with) 
)
ORDER BY date_update
SEGMENTED BY HASH(currency_code) ALL NODES
PARTITION BY date_update::Date
GROUP BY CALENDAR_HIERARCHY_DAY(currencies.date_update::Date, 3, 2);

CREATE PROJECTION IF NOT EXISTS KOSYAK1998YANDEXRU__STAGING.currencies_by_date
(
 date_update,
 currency_code,
 currency_code_with,
 currency_code_div
)
AS
    SELECT currencies.date_update,
        currencies.currency_code,
        currencies.currency_code_with,
        currencies.currency_code_div
    FROM KOSYAK1998YANDEXRU__STAGING.currencies
    ORDER BY currencies.date_update
SEGMENTED BY hash(currencies.date_update) ALL NODES KSAFE 1;