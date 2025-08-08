-- Test: Ensure chargeback rate is reasonable (typically < 5%)
SELECT 
    transaction_date,
    card_country,
    chargeback_rate_pct
FROM {{ ref('fct_tableau_dashboard_master') }}
WHERE chargeback_rate_pct > 5.0  -- Flag unusually high chargeback rates
AND chargeback_transactions > 10  -- Only check meaningful sample sizes
