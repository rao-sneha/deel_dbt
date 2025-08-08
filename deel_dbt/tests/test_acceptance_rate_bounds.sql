-- Test that acceptance rates are between 0 and 100
select aggregation_level, period, acceptance_rate_pct
from {{ ref('fct_acceptance_performance') }}
where acceptance_rate_pct < 0 or acceptance_rate_pct > 100
