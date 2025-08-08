{{ config(materialized='table') }}

WITH accepted_txns AS (
    SELECT
        transaction_ref,
        card_country,
        transaction_age_bucket,
        is_chargeback
    FROM {{ ref('int_transactions_with_chargeback') }}
    WHERE is_accepted = TRUE
),
bucketed_summary AS (
    SELECT
        card_country AS country,
        transaction_age_bucket,
        COUNT(*) AS total_accepted,
        SUM(CASE WHEN is_chargeback THEN 1 ELSE 0 END) AS chargeback_count,
        ROUND(SUM(CASE WHEN is_chargeback THEN 1 ELSE 0 END) / COUNT(*), 4) AS chargeback_rate_pct
    FROM accepted_txns
    GROUP BY card_country, transaction_age_bucket
)
SELECT
    country,
    transaction_age_bucket,
    total_accepted,
    chargeback_count,
    chargeback_rate_pct
FROM bucketed_summary
ORDER BY country, transaction_age_bucket
