{{ config(
    materialized = 'view'
) }}

SELECT
    *
FROM
    {{ source(
        'solana',
        'fact_gov_actions'
    ) }}