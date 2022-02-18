{{ config(
    materialized = 'view',
    tags = ['snowflake', 'algorand_views', 'transaction_participation', 'gold'],
) }}

SELECT
    block_id,
    intra,
    address
FROM
    {{ ref('silver_algorand__transaction_participation') }}
