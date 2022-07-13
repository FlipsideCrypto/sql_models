{{ config(
    materialized = 'view',
    tags = ['snowflake', 'crosschain','swaps'],
    persist_docs ={ "relation": true,
    "columns": true }
) }}

-- Harmony/sushi
SELECT
    'Harmony' as Blockchain,
    Block_timestamp,
    Block_number,
    Tx_hash,
    evm_origin_from_address as origin_from_address,
    evm_origin_to_address as origin_to_address,
    Pool_address,
    Platform,
    Event_index,
    Amount_in,
    Amount_out,
    Sender,
    Log_id,
    Token_in,
    Token_out,
    Symbol_in,
    Symbol_out,
    Tx_to,
    Amount_in_USD,
    Amount_out_USD,
    (Symbol_in || '-' || Symbol_out || ' SLP') as Pool_name

FROM
    {{ source(
        'Harmony_db',
        'EZ_SUSHI_SWAPS'
    ) }} 

Union all


--  Ethereum/All dex
SELECT
    'Ethereum' as Blockchain,
    Block_timestamp,
    Block_number,
    Tx_hash,
    origin_from_address,
    origin_to_address,
    contract_address as Pool_address,
    Platform,
    Event_index,
    Amount_in,
    Amount_out,
    Sender,
    _Log_id as Log_id,
    Token_in,
    Token_out,
    Symbol_in,
    Symbol_out,
    Tx_to,
    Amount_in_USD,
    Amount_out_USD,
    Pool_name
FROM
    {{ source(
        'ethereum_db',
        'ez_dex_swaps'
    ) }}

Union all


--  polygon/sushi
SELECT
    'Polygon' as Blockchain,
    Block_timestamp,
    Block_number,
    Tx_hash,
    origin_from_address,
    origin_to_address,
    contract_address as Pool_address,
    Platform,
    Event_index,
    Amount_in,
    Amount_out,
    Sender,
    _Log_id as Log_id,
    Token_in,
    Token_out,
    Symbol_in,
    Symbol_out,
    Tx_to,
    Amount_in_USD,
    Amount_out_USD,
    Pool_name
FROM
    {{ source(
        'polygon',
        'EZ_SWAPS'
    ) }}

Union all

--  Arbitrum/sushi
SELECT
    'Arbitrum' as Blockchain,
    Block_timestamp,
    Block_number,
    Tx_hash,
    origin_from_address,
    origin_to_address,
    contract_address as Pool_address,
    Platform,
    Event_index,
    Amount_in,
    Amount_out,
    Sender,
    _Log_id as Log_id,
    Token_in,
    Token_out,
    Symbol_in,
    Symbol_out,
    Tx_to,
    Amount_in_USD,
    Amount_out_USD,
    Pool_name
FROM
    {{ source(
        'Arbitrum',
        'EZ_SWAPS'
    ) }}