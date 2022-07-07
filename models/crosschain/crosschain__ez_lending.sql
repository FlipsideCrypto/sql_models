{{ config(
    materialized = 'view',
    tags = ['snowflake', 'crosschain','lending'],
    persist_docs ={ "relation": true,
    "columns": true }
) }}

-- Ethereum/sushi
select
    block_timestamp,
    block_number,
    tx_hash,
    action,
    'ethereum' as blockchain,
    'sushi' as platform,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    asset,
    depositor,
    lender_is_a_contract,
    lending_pool_address,
    event_index,
    amount,
    amount_usd,
    lending_pool,
    symbol,
    _log_id

from
    {{ source(
        'ethereum_db_sushi',
        'ez_lending'
    ) }} 

union all

-- polygon/sushi
select
    block_timestamp,
    block_number,
    tx_hash,
    action,
    'polygon' as blockchain,
    'sushi' as platform,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    asset,
    depositor,
    lender_is_a_contract,
    lending_pool_address,
    event_index,
    amount,
    amount_usd,
    lending_pool,
    symbol,
    _log_id

from
    {{ source(
        'polygon',
        'EZ_LENDING'
    ) }} 