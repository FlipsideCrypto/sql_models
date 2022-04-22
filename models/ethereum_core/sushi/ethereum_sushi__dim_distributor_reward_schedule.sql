{{ config(
    materialized = 'view',
    tags = ['snowflake', 'ethereum', 'gold_ethereum', 'new_eth'],
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    *
FROM
    {{ source(
        'ethereum_db_sushi',
        'dim_distributor_reward_schedule'
    ) }}
