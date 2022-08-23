{{ config(
    materialized = 'view',
    tags = ['snowflake', 'crosschain','borrowing'],
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
    borrower,
    borrower_is_a_contract,
    lending_pool_address,
    event_index,
    amount as asset_amount,
    amount_usd as asset_amount_usd,
    lending_pool,
    symbol,
    collateral_symbol,
    _log_id

from
    {{ source(
        'ethereum_db_sushi',
        'ez_borrowing'
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
    borrower,
    borrower_is_a_contract,
    lending_pool_address,
    event_index,
    amount as asset_amount,
    amount_usd as asset_amount_usd,
    lending_pool,
    symbol,
    collateral_symbol,
    _log_id
from
    {{ source(
        'polygon',
        'EZ_BORROWING'
    ) }} 

union all

-- arbitrum/sushi
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
    borrower,
    borrower_is_a_contract,
    lending_pool_address,
    event_index,
    asset_amount,
    asset_amount_usd,
    lending_pool,
    symbol,
    collateral_symbol,
    _log_id
from
    {{ source(
        'Arbitrum',
        'EZ_BORROWING'
    ) }} 