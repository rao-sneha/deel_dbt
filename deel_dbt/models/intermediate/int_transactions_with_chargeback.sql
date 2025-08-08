{{ config(materialized='table') }}

WITH max_date AS (
    SELECT MAX(txn_date) AS max_txn_date
    FROM {{ ref('stg_globepay__acceptance_report') }}
)

SELECT
    a.transaction_ref,
    cal.date AS transaction_date,
    a.transaction_datetime,
    a.transaction_source,
    a.transaction_state,
    a.transaction_amount,
    a.transaction_currency,
    a.card_country,
    a.cvv_provided,
    a.status AS is_successful,
    a.transaction_fx_rate,
    a.transaction_amount_usd,
    a.transaction_state = 'ACCEPTED' AS is_accepted,
    a.transaction_state = 'DECLINED' AS is_declined,
    COALESCE(c.chargeback, FALSE) AS is_chargeback,
    md.max_txn_date - a.txn_date AS transaction_age_days,
    CASE
        WHEN md.max_txn_date - a.txn_date < 7 THEN '1. 0-6 days'
        WHEN md.max_txn_date - a.txn_date < 30 THEN '2. 7-29 days'
        WHEN md.max_txn_date - a.txn_date < 90 THEN '3. 30-89 days'
        WHEN md.max_txn_date - a.txn_date < 180 THEN '4. 90-179 days'
        ELSE '5. 180+ days'
    END AS transaction_age_bucket,
    CASE
        WHEN a.transaction_amount_usd < 10 THEN '1. <10'
        WHEN a.transaction_amount_usd < 100 THEN '2. 10-99'
        WHEN a.transaction_amount_usd < 500 THEN '3. 100-499'
        WHEN a.transaction_amount_usd < 1000 THEN '4. 500-999'
        WHEN a.transaction_amount_usd < 5000 THEN '5. 1000-4999'
        ELSE '6. 5000+'
    END AS transaction_amount_usd_bucket,
    a.transaction_amount_usd > 1000 AS is_high_value_txn,
    a.transaction_amount_usd > 1000 AND cal.weekend = TRUE AS is_high_risk_time_txn,
    NOT a.cvv_provided AS cvv_missing,
    CASE
        WHEN a.transaction_state = 'DECLINED' AND a.transaction_currency != 'USD' THEN 1
        ELSE 0
    END AS fx_declined,
    CASE
        WHEN COALESCE(c.chargeback, FALSE) AND a.transaction_currency != 'USD' THEN 1
        ELSE 0
    END AS fx_chargeback,
    CASE
        WHEN COALESCE(c.chargeback, FALSE) AND a.transaction_state = 'DECLINED' THEN 1
        ELSE 0
    END AS is_declined_chargeback,
    cal.year AS transaction_year,
    cal.month_name AS transaction_month,
    cal.weekend AS is_weekend_transaction,
    cal.quarter_name,
    cal.first_date_of_month,
    cal.last_date_of_month

FROM {{ ref('stg_globepay__acceptance_report') }} a
LEFT JOIN {{ ref('stg_globepay__chargeback_report') }} c 
    ON a.transaction_ref = c.transaction_ref
LEFT JOIN {{ ref('calendar') }} cal 
    ON cal.date = a.txn_date
CROSS JOIN max_date md
