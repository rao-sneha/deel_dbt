{{ config(materialized='table') }}

WITH src AS (
  SELECT *
  FROM {{ source('globepay', 'acceptance_report') }}
),
renamed AS (
  SELECT
    TRIM(external_ref) AS transaction_ref,
    TRIM(ref) AS globepay_ref,
    CAST(date_time AS TIMESTAMP) AS transaction_datetime,
    LOWER(TRIM(source)) AS transaction_source,
    UPPER(TRIM(state)) AS transaction_state,
    CAST(amount AS REAL) AS transaction_amount,
    UPPER(TRIM(currency)) AS transaction_currency,
    UPPER(TRIM(country)) AS card_country,
    LOWER(TRIM(cvv_provided)) = 'true' AS cvv_provided,
    LOWER(TRIM(status)) = 'true' AS status,
    CAST(rates AS JSONB) AS fx_rates
  FROM src
  WHERE TRIM(external_ref) IS NOT NULL
),
transformed AS (
  SELECT
    transaction_ref,
    globepay_ref,
    transaction_datetime,
    transaction_datetime::DATE AS txn_date,
    transaction_source,
    transaction_state,
    transaction_amount,
    transaction_currency,
    card_country,
    cvv_provided,
    status,
    fx_rates,
    CASE 
      WHEN fx_rates IS NOT NULL AND (fx_rates ->> transaction_currency) IS NOT NULL
      THEN CAST(fx_rates ->> transaction_currency AS NUMERIC)
      ELSE NULL
    END AS transaction_fx_rate,
    CASE
      WHEN fx_rates IS NOT NULL AND (fx_rates ->> transaction_currency) IS NOT NULL
        AND CAST(fx_rates ->> transaction_currency AS NUMERIC) > 0
      THEN CAST(transaction_amount / CAST(fx_rates ->> transaction_currency AS NUMERIC) AS DECIMAL(10,2))
      ELSE NULL
    END AS transaction_amount_usd
  FROM renamed
)
SELECT *
FROM transformed
