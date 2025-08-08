-- Test: Ensure acceptance + calculated decline rates sum to approximately 100%
SELECT 
    aggregation_level,
    period,
    acceptance_rate_pct,
    ROUND(declined_transactions * 100.0 / NULLIF(total_transactions, 0), 2) AS decline_rate_pct,
    ABS(acceptance_rate_pct + ROUND(declined_transactions * 100.0 / NULLIF(total_transactions, 0), 2) - 100) AS rate_difference
FROM {{ ref('fct_acceptance_performance') }}
WHERE ABS(acceptance_rate_pct + ROUND(declined_transactions * 100.0 / NULLIF(total_transactions, 0), 2) - 100) > 0.1  -- Allow 0.1% tolerance for rounding
