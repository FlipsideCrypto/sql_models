{{ config(
    materialized = 'view', 
    tags = ['snowflake', 'solana', 'gold_solana', 'solana_transfers', 'solana_user_transfers']
) }}

SELECT
    block_timestamp, 
    block_id, 
    blockchain, 
    recent_block_hash, 
    tx_id, 
    index, 
    event_type, 
    preTokenBalances, 
    postTokenBalances, 
    instruction:parsed:info:destination :: STRING AS destination, 
    instruction:parsed:info:source :: STRING AS source, 
    instruction:parsed:info:authority :: STRING AS authority,
    CASE 
        WHEN event_type = 'transferChecked' THEN instruction:parsed:info:tokenAmount:amount/POW(10,6)
        WHEN event_type = 'transfer' AND instruction:programId :: STRING = '11111111111111111111111111111111' THEN instruction:parsed:info:lamports/POW(10,9)
        ELSE instruction:parsed:info:amount/POW(10,6) END
    AS amount, 
    instruction:programId :: STRING AS program_id, 
    succeeded, 
    ingested_at, 
    CASE WHEN program_id <> 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA' THEN TRUE ELSE FALSE END AS transfer_tx_flag 
FROM {{ ref('silver_solana__events') }}

WHERE event_type = 'transfer' 
OR event_type = 'transferChecked'
