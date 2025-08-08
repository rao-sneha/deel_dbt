-- Test: Ensure curated models have reasonable data coverage
SELECT 
    'fct_acceptance_performance' AS model_name,
    MAX(period) AS latest_date,
    CURRENT_DATE - MAX(period) AS days_since_latest
FROM {{ ref('fct_acceptance_performance') }}
WHERE aggregation_level = 'daily'
HAVING CURRENT_DATE - MAX(period) > 3650  -- Flag if data is more than 10 years old (unreasonable) but just for the task, Ideally should be based on data ingestion frequency 

UNION ALL

SELECT 
    'fct_tableau_dashboard_master' AS model_name,
    MAX(transaction_date) AS latest_date,
    CURRENT_DATE - MAX(transaction_date) AS days_since_latest
FROM {{ ref('fct_tableau_dashboard_master') }}
HAVING CURRENT_DATE - MAX(transaction_date) > 3650  -- Flag if data is more than 10 years old (unreasonable) but just for the task, Ideally should be based on data ingestion frequency
