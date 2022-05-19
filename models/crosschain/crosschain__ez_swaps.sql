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
    (Symbol_in || '-' || Symbol_out || ' SLP') as Pool_name,
    Ingested_at

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
    Pool_name,
    Ingested_at
FROM
    {{ source(
        'ethereum_db',
        'ez_dex_swaps'
    ) }}
