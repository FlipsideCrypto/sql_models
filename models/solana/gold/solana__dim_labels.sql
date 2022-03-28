{{ config(
    materialized = 'view'
) }}

SELECT
    *
FROM
    {{ source(
        'solana',
        'dim_labels'
    ) }}
