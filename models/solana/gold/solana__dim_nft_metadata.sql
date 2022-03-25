{{ config(
    materialized = 'view'
) }}

SELECT
    *
FROM
    {{ source(
        'solana',
        'dim_nft_metadata'
    ) }}
