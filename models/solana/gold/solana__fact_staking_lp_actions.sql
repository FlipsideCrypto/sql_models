{{ config(
    materialized = 'view'
) }}

SELECT
    *
FROM
    {{ source(
        'solana',
        'fact_staking_lp_actions'
    ) }}
