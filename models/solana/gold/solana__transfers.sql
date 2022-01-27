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
    event_type, 
    instruction[0]:parsed:info:destination :: STRING AS destination, 
    instruction[0]:parsed:info:source :: STRING AS source, 
    instruction[0]:parsed:info:authority :: STRING AS authority,
    CASE 
        WHEN event_type = 'transferChecked' THEN instruction[0]:parsed:info:tokenAmount:amount/POW(10,6)
        WHEN event_type = 'transfer' AND instruction[0]:programId :: STRING = '11111111111111111111111111111111' THEN instruction[0]:parsed:info:lamports/POW(10,9)
        ELSE instruction[0]:parsed:info:amount/POW(10,6) END
    AS amount, 
    instruction[0]:programId :: STRING AS program_id, 
    succeeded, 
    ingested_at, 
    CASE WHEN program_id <> 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA' THEN TRUE ELSE FALSE END AS transfer_tx_flag 
FROM {{ ref('silver_solana__events') }}

WHERE event_type = 'transfer' 
OR event_type = 'transferChecked'

qualify(ROW_NUMBER() over(PARTITION BY block_id, tx_id
ORDER BY
  ingested_at DESC)) = 1