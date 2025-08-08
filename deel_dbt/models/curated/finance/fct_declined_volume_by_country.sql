{{ config(materialized='table') }}

WITH declined_txns AS (
    SELECT
        card_country,
        transaction_amount_usd
    FROM {{ ref('int_transactions_with_chargeback') }}
    WHERE is_declined = TRUE
      AND transaction_amount_usd IS NOT NULL
),
total_volume AS (
    SELECT SUM(transaction_amount_usd) AS global_declined_volume_usd
    FROM declined_txns
),
declined_by_country AS (
    SELECT
        card_country AS country,
        COUNT(*) AS declined_transaction_count,
        SUM(transaction_amount_usd) AS total_declined_usd,
        ROUND(SUM(transaction_amount_usd) / 1000000.0, 2) AS total_declined_millions_usd,
        ROUND(AVG(transaction_amount_usd), 2) AS avg_declined_txn_usd
    FROM declined_txns
    GROUP BY card_country
    HAVING SUM(transaction_amount_usd) > 25000000
),
enriched AS (
    SELECT
        d.country,
        d.declined_transaction_count,
        d.total_declined_millions_usd,
        d.avg_declined_txn_usd,
        ROUND(d.total_declined_usd / t.global_declined_volume_usd, 4) AS declined_volume_pct_of_global
    FROM declined_by_country d
    CROSS JOIN total_volume t
)
SELECT
    country,
    declined_transaction_count,
    total_declined_millions_usd,
    avg_declined_txn_usd,
    declined_volume_pct_of_global,
    CONCAT('$', CAST(total_declined_millions_usd AS TEXT), 'M') AS formatted_amount
FROM enriched
ORDER BY total_declined_millions_usd DESC
