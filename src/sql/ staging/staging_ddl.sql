DROP TABLE IF EXISTS KOSYAK1998YANDEXRU__STAGING.transactions;

CREATE TABLE KOSYAK1998YANDEXRU__STAGING.transactions (
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


DROP TABLE IF NOT EXISTS KOSYAK1998YANDEXRU__STAGING.currencies;

CREATE TABLE KOSYAK1998YANDEXRU__STAGING.currencies (
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