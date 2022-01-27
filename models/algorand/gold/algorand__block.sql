{{ config(
    materialized = 'view',
    tags = ['snowflake', 'algorand_views', 'block', 'gold'],
) }}

SELECT
    block_id,
    block_timestamp,
    rewardslevel,
    network,
    genesis_hash,
    prev_block_hash,
    txn_root,
    header
FROM
    {{ ref('silver_algorand__block') }}
