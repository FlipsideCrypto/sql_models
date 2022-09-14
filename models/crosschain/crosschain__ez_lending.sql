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
    amount as asset_amount,
    amount_usd as asset_amount_usd,
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
    amount as asset_amount,
    amount_usd as asset_amount_usd,
    lending_pool,
    symbol,
    _log_id

from
    {{ source(
        'polygon',
        'EZ_LENDING'
    ) }} 

union all

-- Arbitrum/sushi
select
    block_timestamp,
    block_number,
    tx_hash,
    action,
    'arbitrum' as blockchain,
    'sushi' as platform,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    asset,
    depositor,
    lender_is_a_contract,
    lending_pool_address,
    asset_amount,
    asset_amount_usd,
    lending_pool,
    symbol,
    _log_id

from
    {{ source(
        'Arbitrum',
        'EZ_LENDING'
    ) }} 

union all

-- Avalanche/sushi
select
    block_timestamp,
    block_number,
    tx_hash,
    action,
    'avalanche' as blockchain,
    'sushi' as platform,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    asset,
    depositor,
    lender_is_a_contract,
    lending_pool_address,
    asset_amount,
    asset_amount_usd,
    lending_pool,
    symbol,
    _log_id

from
    {{ source(
        'Avalanche',
        'EZ_LENDING'
    ) }} 

union all

-- BSC/sushi
select
    block_timestamp,
    block_number,
    tx_hash,
    action,
    'BSC' as blockchain,
    'sushi' as platform,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    asset,
    depositor,
    lender_is_a_contract,
    lending_pool_address,
    asset_amount,
    asset_amount_usd,
    lending_pool,
    symbol,
    _log_id

from
    {{ source(
        'BSC',
        'EZ_LENDING'
    ) }} 

union all

-- Gnosis/sushi
select
    block_timestamp,
    block_number,
    tx_hash,
    action,
    'gnosis' as blockchain,
    'sushi' as platform,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    asset,
    depositor,
    lender_is_a_contract,
    lending_pool_address,
    asset_amount,
    asset_amount_usd,
    lending_pool,
    symbol,
    _log_id

from
    {{ source(
        'Gnosis',
        'EZ_LENDING'
    ) }} 