{{ config(materialized='table') }}

WITH calendar AS (
    SELECT 
        date,
        day_of_week,
        weekend
    FROM {{ ref('calendar') }}
),

comprehensive_metrics AS (
    SELECT
        t.transaction_date,
        t.card_country,
        t.transaction_currency,
        t.transaction_source,
        t.transaction_age_bucket,
        t.transaction_amount_usd_bucket,
        
        -- Time dimensions from calendar
        EXTRACT(HOUR FROM t.transaction_datetime) AS transaction_hour,
        CASE WHEN cal.weekend THEN 1 ELSE 0 END AS day_of_week, -- Using calendar weekend flag
        CASE 
            WHEN cal.weekend THEN 'Weekend'
            ELSE 'Weekday'
        END AS day_type,
        CASE 
            WHEN EXTRACT(HOUR FROM t.transaction_datetime) BETWEEN 6 AND 11 THEN 'Morning (6-11)'
            WHEN EXTRACT(HOUR FROM t.transaction_datetime) BETWEEN 12 AND 17 THEN 'Afternoon (12-17)'
            WHEN EXTRACT(HOUR FROM t.transaction_datetime) BETWEEN 18 AND 23 THEN 'Evening (18-23)'
            ELSE 'Night (0-5)'
        END AS time_period,
        
        -- Core transaction metrics
        COUNT(*) AS total_transactions,
        SUM(CASE WHEN t.is_accepted THEN 1 ELSE 0 END) AS accepted_transactions,
        SUM(CASE WHEN t.is_declined THEN 1 ELSE 0 END) AS declined_transactions,
        SUM(CASE WHEN t.is_chargeback THEN 1 ELSE 0 END) AS chargeback_transactions,
        
        -- Volume metrics (Local Currency)
        SUM(t.transaction_amount) AS total_volume_local,
        SUM(CASE WHEN t.is_accepted THEN t.transaction_amount ELSE 0 END) AS accepted_volume_local,
        SUM(CASE WHEN t.is_declined THEN t.transaction_amount ELSE 0 END) AS declined_volume_local,
        
        -- Volume metrics (USD)
        SUM(t.transaction_amount_usd) AS total_volume_usd,
        SUM(CASE WHEN t.is_accepted THEN t.transaction_amount_usd ELSE 0 END) AS accepted_volume_usd,
        SUM(CASE WHEN t.is_declined THEN t.transaction_amount_usd ELSE 0 END) AS declined_volume_usd,
        SUM(CASE WHEN t.is_chargeback THEN t.transaction_amount_usd ELSE 0 END) AS chargeback_volume_usd,
        
        -- Risk metrics
        SUM(CASE WHEN t.is_high_value_txn THEN 1 ELSE 0 END) AS high_value_transactions,
        SUM(CASE WHEN t.cvv_missing THEN 1 ELSE 0 END) AS cvv_missing_transactions,
        SUM(CASE WHEN t.is_weekend_transaction THEN 1 ELSE 0 END) AS weekend_transactions,
        SUM(CASE WHEN t.is_high_risk_time_txn THEN 1 ELSE 0 END) AS high_risk_time_transactions,
        SUM(CASE WHEN t.fx_declined > 0 THEN 1 ELSE 0 END) AS fx_declined_transactions,
        SUM(CASE WHEN t.fx_chargeback > 0 THEN 1 ELSE 0 END) AS fx_chargeback_transactions,
        
        -- Combined risk scenarios
        SUM(CASE WHEN t.cvv_missing AND t.is_high_value_txn THEN 1 ELSE 0 END) AS high_value_no_cvv_transactions,
        SUM(CASE WHEN t.is_weekend_transaction AND t.is_high_value_txn THEN 1 ELSE 0 END) AS weekend_high_value_transactions,
        SUM(CASE WHEN t.cvv_missing AND t.is_chargeback THEN 1 ELSE 0 END) AS cvv_missing_chargebacks,
        
        -- FX metrics
        AVG(t.transaction_fx_rate) AS avg_fx_rate,
        MIN(t.transaction_fx_rate) AS min_fx_rate,
        MAX(t.transaction_fx_rate) AS max_fx_rate,
        
        -- Average transaction sizes
        AVG(CASE WHEN t.is_accepted THEN t.transaction_amount_usd END) AS avg_accepted_txn_size_usd,
        AVG(CASE WHEN t.is_declined THEN t.transaction_amount_usd END) AS avg_declined_txn_size_usd,
        
        -- Country diversity (for currency analysis)
        COUNT(DISTINCT CASE WHEN t.transaction_currency = 'USD' THEN NULL ELSE t.card_country END) AS non_usd_countries_count
        
    FROM {{ ref('int_transactions_with_chargeback') }} t
    LEFT JOIN calendar cal ON cal.date = t.transaction_date
    GROUP BY 
        t.transaction_date, 
        t.card_country, 
        t.transaction_currency, 
        t.transaction_source, 
        t.transaction_age_bucket, 
        t.transaction_amount_usd_bucket,
        EXTRACT(HOUR FROM t.transaction_datetime),
        cal.weekend
)

SELECT
    *,
    -- Core performance KPIs
    ROUND(accepted_transactions * 100.0 / NULLIF(total_transactions, 0), 2) AS acceptance_rate_pct,
    ROUND(declined_transactions * 100.0 / NULLIF(total_transactions, 0), 2) AS decline_rate_pct,
    ROUND(chargeback_transactions * 100.0 / NULLIF(accepted_transactions, 0), 4) AS chargeback_rate_pct,
    
    -- Volume-based KPIs
    ROUND(accepted_volume_usd * 100.0 / NULLIF(total_volume_usd, 0), 2) AS acceptance_volume_pct,
    ROUND(chargeback_volume_usd / NULLIF(total_volume_usd, 0) * 100, 4) AS chargeback_volume_rate_pct,
    
    -- Risk KPIs
    ROUND(high_value_transactions * 100.0 / NULLIF(total_transactions, 0), 2) AS high_value_rate_pct,
    ROUND(cvv_missing_transactions * 100.0 / NULLIF(total_transactions, 0), 2) AS cvv_missing_rate_pct,
    ROUND(weekend_transactions * 100.0 / NULLIF(total_transactions, 0), 2) AS weekend_rate_pct,
    ROUND(high_risk_time_transactions * 100.0 / NULLIF(total_transactions, 0), 2) AS high_risk_time_rate_pct,
    
    -- Advanced risk scenarios
    ROUND(high_value_no_cvv_transactions * 100.0 / NULLIF(high_value_transactions, 0), 2) AS high_value_no_cvv_rate_pct,
    ROUND(weekend_high_value_transactions * 100.0 / NULLIF(weekend_transactions, 0), 2) AS weekend_high_value_rate_pct,
    ROUND(cvv_missing_chargebacks * 100.0 / NULLIF(cvv_missing_transactions, 0), 4) AS cvv_missing_chargeback_rate_pct,
    
    -- FX impact analysis
    ROUND(CAST((total_volume_local - total_volume_usd) / NULLIF(total_volume_local, 0) * 100 AS NUMERIC), 2) AS fx_impact_pct,
    ROUND(CAST(fx_declined_transactions * 100.0 / NULLIF(declined_transactions, 0) AS NUMERIC), 2) AS fx_decline_contribution_pct,
    ROUND(CAST(fx_chargeback_transactions * 100.0 / NULLIF(chargeback_transactions, 0) AS NUMERIC), 2) AS fx_chargeback_contribution_pct,
    
    -- Operational metrics
    ROUND(total_volume_usd / 1000000.0, 2) AS total_volume_millions_usd,
    ROUND(accepted_volume_usd / 1000000.0, 2) AS accepted_volume_millions_usd,
    ROUND(chargeback_volume_usd / 1000000.0, 2) AS chargeback_volume_millions_usd,
    
    -- Time-based performance flags
    CASE WHEN transaction_hour BETWEEN 9 AND 17 THEN 1 ELSE 0 END AS is_business_hours,
    CASE WHEN day_of_week = 1 THEN 1 ELSE 0 END AS is_weekend_flag

FROM comprehensive_metrics
ORDER BY transaction_date, card_country, transaction_currency, transaction_source, transaction_hour
