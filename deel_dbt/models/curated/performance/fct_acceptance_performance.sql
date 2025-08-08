{{ config(materialized='table') }}

WITH base AS (
    SELECT
        transaction_date,
        transaction_datetime,
        is_accepted,
        is_declined,
        transaction_amount_usd,
        is_weekend_transaction
    FROM {{ ref('int_transactions_with_chargeback') }}
),

calendar AS (
    SELECT 
        date,
        first_date_of_month,
        DATE_TRUNC('week', date) AS first_date_of_week
    FROM {{ ref('calendar') }}
),

aggregated AS (
    SELECT
        aggregation_level,
        CAST(period AS DATE) AS period,
        COUNT(*) AS total_transactions,
        SUM(CASE WHEN is_accepted THEN 1 ELSE 0 END) AS accepted_transactions,
        SUM(CASE WHEN is_declined THEN 1 ELSE 0 END) AS declined_transactions,

        -- USD Volume
        SUM(transaction_amount_usd) AS total_volume_usd,
        SUM(CASE WHEN is_accepted THEN transaction_amount_usd ELSE 0 END) AS accepted_volume_usd,
        SUM(CASE WHEN is_declined THEN transaction_amount_usd ELSE 0 END) AS declined_volume_usd,

        -- Weekend counts
        SUM(CASE WHEN is_weekend_transaction AND is_accepted THEN 1 ELSE 0 END) AS weekend_accepted_txns,
        SUM(CASE WHEN is_weekend_transaction AND is_declined THEN 1 ELSE 0 END) AS weekend_declined_txns,

        -- Weekend volume
        SUM(CASE WHEN is_weekend_transaction THEN transaction_amount_usd ELSE 0 END) AS weekend_total_volume_usd,
        SUM(CASE WHEN is_weekend_transaction AND is_accepted THEN transaction_amount_usd ELSE 0 END) AS weekend_accepted_volume_usd,
        SUM(CASE WHEN is_weekend_transaction AND is_declined THEN transaction_amount_usd ELSE 0 END) AS weekend_declined_volume_usd

    FROM (
        -- Daily
        SELECT
            'daily' AS aggregation_level,
            transaction_date AS period,
            *
        FROM base

        UNION ALL

        -- Weekly (using calendar)
        SELECT
            'weekly' AS aggregation_level,
            cal.first_date_of_week AS period,
            b.*
        FROM base b
        LEFT JOIN calendar cal ON cal.date = b.transaction_date
        
        UNION ALL
        
        -- Monthly (using calendar)
        SELECT
            'monthly' AS aggregation_level,
            cal.first_date_of_month AS period,
            b.*
        FROM base b
        LEFT JOIN calendar cal ON cal.date = b.transaction_date
    ) AS unioned
    GROUP BY aggregation_level, period
)

SELECT
    *,
    -- Acceptance % by count
    ROUND(accepted_transactions * 100.0 / NULLIF(total_transactions, 0), 2) AS acceptance_rate_pct,
    
    -- Acceptance % by volume
    ROUND(accepted_volume_usd * 100.0 / NULLIF(total_volume_usd, 0), 2) AS acceptance_volume_pct,
    
    -- Weekend acceptance % (count)
    ROUND(weekend_accepted_txns * 100.0 / NULLIF(weekend_accepted_txns + weekend_declined_txns, 0), 2) AS weekend_acceptance_rate_pct,

    -- Weekend acceptance % (volume)
    ROUND(weekend_accepted_volume_usd * 100.0 / NULLIF(weekend_total_volume_usd, 0), 2) AS weekend_acceptance_volume_pct
FROM aggregated
ORDER BY aggregation_level, period
