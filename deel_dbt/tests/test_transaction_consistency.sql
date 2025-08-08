-- Test that total transactions equal accepted plus declined
select aggregation_level, period
from {{ ref('fct_acceptance_performance') }}
where total_transactions != (accepted_transactions + declined_transactions)
