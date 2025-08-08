{% macro calculate_acceptance_rate(accepted_col, total_col, precision=2) %}
    ROUND({{ accepted_col }} * 100.0 / NULLIF({{ total_col }}, 0), {{ precision }})
{% endmacro %}

{% macro bucket_transaction_amount(amount_col, currency='USD') %}
    CASE
        WHEN {{ amount_col }} < 10 THEN '1. <10'
        WHEN {{ amount_col }} < 100 THEN '2. 10-99'
        WHEN {{ amount_col }} < 500 THEN '3. 100-499'
        WHEN {{ amount_col }} < 1000 THEN '4. 500-999'
        WHEN {{ amount_col }} < 5000 THEN '5. 1000-4999'
        ELSE '6. 5000+'
    END
{% endmacro %}

{% macro bucket_transaction_age(date_col, reference_date=None) %}
    CASE
        WHEN DATE_DIFF(
            {% if reference_date %}{{ reference_date }}{% else %}CURRENT_DATE(){% endif %}, 
            {{ date_col }}, DAY
        ) < 7 THEN '1. 0-6 days'
        WHEN DATE_DIFF(
            {% if reference_date %}{{ reference_date }}{% else %}CURRENT_DATE(){% endif %}, 
            {{ date_col }}, DAY
        ) < 30 THEN '2. 7-29 days'
        WHEN DATE_DIFF(
            {% if reference_date %}{{ reference_date }}{% else %}CURRENT_DATE(){% endif %}, 
            {{ date_col }}, DAY
        ) < 90 THEN '3. 30-89 days'
        WHEN DATE_DIFF(
            {% if reference_date %}{{ reference_date }}{% else %}CURRENT_DATE(){% endif %}, 
            {{ date_col }}, DAY
        ) < 180 THEN '4. 90-179 days'
        ELSE '5. 180+ days'
    END
{% endmacro %}
