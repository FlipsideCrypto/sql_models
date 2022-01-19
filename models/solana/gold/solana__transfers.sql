{{ config(
    materialized = 'view', 
    tags = ['snowflake', 'solana', 'gold_solana', 'solana_transfers', 'solana_user_transfers']
) }}

SELECT
    block_timestamp, 
    block_id,
    blockchain, 
    recent_blockhash, 
    tx_id, 
    ei.value:parsed:info:authority :: STRING AS tx_from_address,
    ei.value:parsed:info:destination :: STRING AS tx_to_address, 
    ei.value:parsed:info:amount/POW(10,9) :: INTEGER AS solana_amount, 
    succeeded, 
    ei.value:programId :: STRING AS program_id,
    ingested_at, 
    CASE WHEN program_id <> 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA' THEN TRUE ELSE FALSE END AS transfer_tx_flag 
FROM {{ ref('silver_solana__events') }}, 

LATERAL FLATTEN (
    input => event_info ) ei

WHERE ei.value:parsed:type :: STRING = 'transfer'
AND ei.value:program :: STRING = 'spl-token'
