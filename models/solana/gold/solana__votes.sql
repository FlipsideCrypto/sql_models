{{ config(
    materialized = 'view', 
    tags = ['snowflake', 'solana', 'gold_solana', 'solana_events', 'solana_votes']
) }}

SELECT 
    block_timestamp, 
    block_id, 
    e.blockchain, 
    recent_blockhash, 
    tx_id, 
    succeeded,  
    event_info:program :: STRING AS program_type, 
    event_info:programId :: STRING AS program_id, 
    contract_labels.project_name :: STRING AS program_name, 
    event_info:parsed:info:voteAccount :: STRING AS vote_account, 
    event_info:parsed:info:voteAuthority :: STRING AS vote_authority, 
    ingested_at, 
    transfer_tx_flag
FROM {{ ref('silver_solana__events') }} e

LEFT OUTER JOIN {{ ref('silver_solana__contract_names') }} AS contract_labels
ON event_info:programId :: STRING COLLATE 'en-ci' = contract_labels.address AND contract_labels.blockchain = 'solana'

WHERE event_info:program = 'vote' 