-- H_TRANSACTION

DROP TABLE IF EXISTS KOSYAK1998YANDEXRU__DWH.h_transaction;

CREATE TABLE KOSYAK1998YANDEXRU__DWH.h_transaction (
    hk_transaction_id integer NOT NULL,
    operation_id uuid NOT NULL,
    load_dt timestamp(0) NOT NULL,
    load_src varchar(20) NOT NULL,
    CONSTRAINT hk_transaction_id_pkey PRIMARY KEY (hk_transaction_id) 
)
ORDER BY load_dt
SEGMENTED BY hk_transaction_id ALL NODES
PARTITION BY load_dt::Date
GROUP BY CALENDAR_HIERARCHY_DAY(h_transaction.load_dt::Date, 3, 2);

-- H_ACCOUNT

DROP TABLE IF EXISTS KOSYAK1998YANDEXRU__DWH.h_account;

CREATE TABLE KOSYAK1998YANDEXRU__DWH.h_account (
    hk_account_id integer NOT NULL,
    account_id integer NOT NULL,
    load_dt timestamp(0) NOT NULL,
    load_src varchar(20) NOT NULL,
    CONSTRAINT hk_account_id_pkey PRIMARY KEY (hk_account_id) 
)
ORDER BY load_dt
SEGMENTED BY hk_account_id ALL NODES
PARTITION BY load_dt::Date
GROUP BY CALENDAR_HIERARCHY_DAY(h_account.load_dt::Date, 3, 2);

-- H_CURRENCY

DROP TABLE IF EXISTS KOSYAK1998YANDEXRU__DWH.h_currency;

CREATE TABLE KOSYAK1998YANDEXRU__DWH.h_currency (
    hk_currency_id integer NOT NULL,
    currency_id integer NOT NULL,
    load_dt timestamp(0) NOT NULL,
    load_src varchar(20) NOT NULL,
    CONSTRAINT hk_currency_id_pkey PRIMARY KEY (hk_currency_id) 
)
ORDER BY load_dt
SEGMENTED BY hk_currency_id ALL NODES
PARTITION BY load_dt::Date
GROUP BY CALENDAR_HIERARCHY_DAY(h_currency.load_dt::Date, 3, 2);

-- L_TRANSACTIOM_ACCOUNT_FROM_TO

DROP TABLE IF EXISTS KOSYAK1998YANDEXRU__DWH.l_transaction_account_from_to;

CREATE TABLE KOSYAK1998YANDEXRU__DWH.l_transaction_account_from_to (
    hk_l_transaction_account_from_to integer NOT NULL,
    hk_transaction_id integer NOT NULL CONSTRAINT l_transaction_account_from_to_transaction_id_fkey REFERENCES KOSYAK1998YANDEXRU__DWH.h_transaction (hk_transaction_id),
    hk_account_id_from integer NOT NULL CONSTRAINT l_transaction_account_from_to_hk_account_id_from_fkey REFERENCES KOSYAK1998YANDEXRU__DWH.h_account (hk_account_id),
    hk_account_id_to integer NOT NULL CONSTRAINT l_transaction_account_from_to_hk_account_id_to_fkey REFERENCES KOSYAK1998YANDEXRU__DWH.h_account (hk_account_id),
    load_dt timestamp(0) NOT NULL,
    load_src varchar(20) NOT NULL,
    CONSTRAINT hk_l_transaction_account_from_to_pkey PRIMARY KEY (hk_l_transaction_account_from_to)
)
ORDER BY load_dt
SEGMENTED BY hk_l_transaction_account_from_to ALL NODES
PARTITION BY load_dt::Date
GROUP BY CALENDAR_HIERARCHY_DAY(l_transaction_account_from_to.load_dt::Date, 3, 2);

-- L_TRANSACTION_CURRENCY

DROP TABLE IF EXISTS KOSYAK1998YANDEXRU__DWH.l_transaction_currency;

CREATE TABLE KOSYAK1998YANDEXRU__DWH.l_transaction_currency (
    hk_l_transaction_currency integer NOT NULL,
    hk_transaction_id integer NOT NULL CONSTRAINT l_transaction_currency_transaction_id_fkey REFERENCES KOSYAK1998YANDEXRU__DWH.h_transaction (hk_transaction_id),
    hk_currency_id integer NOT NULL CONSTRAINT l_transaction_currency_currency_id_fkey REFERENCES KOSYAK1998YANDEXRU__DWH.h_currency (hk_currency_id),
    load_dt timestamp(0) NOT NULL,
    load_src varchar(20) NOT NULL,
    CONSTRAINT hk_l_transaction_currency_pkey PRIMARY KEY (hk_l_transaction_currency) 
)
ORDER BY load_dt
SEGMENTED BY hk_l_transaction_currency ALL NODES
PARTITION BY load_dt::Date
GROUP BY CALENDAR_HIERARCHY_DAY(l_transaction_currency.load_dt::Date, 3, 2);

-- S_TRANSACTION_REQUISITES

DROP TABLE IF EXISTS KOSYAK1998YANDEXRU__DWH.s_transaction_requisites;

CREATE TABLE KOSYAK1998YANDEXRU__DWH.s_transaction_requisites (
    hk_transaction_id integer NOT NULL,
    "status" varchar(20) NOT NULL,
    amount integer NOT NULL,
    transaction_dt timestamp(3) NOT NULL,
    load_dt timestamp(0) NOT NULL,
    load_src varchar(20) NOT NULL,
    CONSTRAINT operation_id_pkey PRIMARY KEY (hk_transaction_id) 
)
ORDER BY transaction_dt
SEGMENTED BY HASH(transaction_dt::Date) ALL NODES
PARTITION BY transaction_dt::Date
GROUP BY CALENDAR_HIERARCHY_DAY(s_transaction_requisites.transaction_dt::Date, 3, 2);

-- S_CURRENCY_DIV

DROP TABLE IF EXISTS KOSYAK1998YANDEXRU__DWH.s_currency_div;

CREATE TABLE KOSYAK1998YANDEXRU__DWH.s_currency_div (
    hk_currency_id integer NOT NULL,
    hk_currency_id_with integer NOT NULL,
    date_update timestamp(3) NOT NULL,
    currency_div numeric(12, 3) NOT NULL,
    load_dt timestamp(0) NOT NULL,
    load_src varchar(20) NOT NULL,
    CONSTRAINT operation_id_pkey PRIMARY KEY (hk_currency_id, hk_currency_id_with, date_update) 
)
ORDER BY date_update
SEGMENTED BY HASH(date_update::Date) ALL NODES
PARTITION BY date_update::Date
GROUP BY CALENDAR_HIERARCHY_DAY(s_currency_div.date_update::Date, 3, 2);