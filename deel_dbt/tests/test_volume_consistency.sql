-- Test: Ensure volume metrics are consistent
SELECT 
    aggregation_level,
    period,
    total_volume_usd,
    accepted_volume_usd + declined_volume_usd AS calculated_total_volume
FROM {{ ref('fct_acceptance_performance') }}
WHERE ABS(total_volume_usd - (accepted_volume_usd + declined_volume_usd)) > 0.01  -- Allow small rounding differences
