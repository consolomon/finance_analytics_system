CREATE TABLE KOSYAK1998YANDEXRU__DWH.global_metrics (
    date_update timestamp(0) NOT NULL,
    currency_from integer NOT NULL,
    amount_total integer NOT NULL,
    cnt_transactions integer NOT NULL,
    avg_transactions_per_account integer NOT NULL,
    cnt_accounts_make_transactions integer NOT NULL,
    CONSTRAINT global_metrics_date_update_pkey PRIMARY KEY (date_update)
)
ORDER BY date_update
SEGMENTED BY HASH(currency_from) ALL NODES
PARTITION BY date_update::Date
GROUP BY CALENDAR_HIERARCHY_DAY(global_metrics.date_update::Date, 3, 2);