-- Test: Ensure weekend transactions align with calendar
SELECT 
    t.transaction_date,
    t.weekend_transactions,
    t.total_transactions,
    c.weekend
FROM {{ ref('fct_tableau_dashboard_master') }} t
LEFT JOIN {{ ref('calendar') }} c ON c.date = t.transaction_date
WHERE (c.weekend = TRUE AND t.weekend_transactions = 0 AND t.total_transactions > 0)
   OR (c.weekend = FALSE AND t.weekend_transactions > 0)
