{{ config(materialized='table') }}

WITH src AS (
    SELECT *
    FROM {{ source('globepay', 'chargeback_report') }}
),
renamed AS (
    SELECT
        TRIM(external_ref) AS transaction_ref,
        LOWER(TRIM(source)) AS transaction_source,
        LOWER(TRIM(chargeback)) = 'true' AS chargeback,
        LOWER(TRIM(status)) = 'true' AS status
    FROM src
    WHERE TRIM(external_ref) IS NOT NULL
)
SELECT *
FROM renamed
