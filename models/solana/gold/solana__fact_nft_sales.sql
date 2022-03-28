{{ config(
    materialized = 'view'
) }}

SELECT
    *
FROM
    {{ source(
        'solana',
        'fact_nft_sales'
    ) }}
