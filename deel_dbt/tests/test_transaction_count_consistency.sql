-- Test: Ensure transaction counts are consistent
SELECT 
    transaction_date,
    card_country,
    total_transactions,
    accepted_transactions + declined_transactions AS calculated_total
FROM {{ ref('fct_tableau_dashboard_master') }}
WHERE total_transactions != (accepted_transactions + declined_transactions)
