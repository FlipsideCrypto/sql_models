{{ config(
    materialized = 'view', 
    tags = ['snowflake', 'solana', 'gold_solana', 'solana_transfers', 'solana_user_transfers']
) }}

WITH base_table AS (
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
        ingested_at 
    FROM {{ ref('silver_solana__events') }}, 

    LATERAL FLATTEN (
        input => event_info ) ei

    WHERE ei.value:parsed:type :: STRING = 'transfer'
    AND ei.value:program :: STRING = 'spl-token'

)

SELECT 
    block_timestamp, 
    block_id, 
    e.blockchain,
    recent_blockhash, 
    tx_id, 
    e.tx_from_address, 
    from_labels.label_type AS tx_from_label, 
    from_labels.label_subtype AS tx_from_label_subtype, 
    from_labels.program_name as tx_from_program_name,
    from_labels.project_name as tx_from_address_name,
    e.tx_to_address, 
     to_labels.label_type AS tx_to_label, 
    to_labels.label_subtype AS tx_to_label_subtype, 
    to_labels.program_name as tx_to_program_name,
    to_labels.project_name as tx_to_address_name,
    solana_amount, 
    succeeded, 
    e.program_id, 
    contract_labels.project_name AS program_name, 
    ingested_at,
    CASE WHEN program_id <> 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA' THEN TRUE ELSE FALSE END AS transfer_tx_flag
FROM base_table e

LEFT OUTER JOIN {{ ref('silver_solana__contract_names') }} AS contract_labels
ON e.program_id COLLATE 'en-ci' = contract_labels.address AND contract_labels.blockchain = 'solana'

LEFT OUTER JOIN {{ ref('silver_solana__contract_names') }} AS to_labels
ON e.tx_to_address COLLATE 'en-ci' = to_labels.address AND to_labels.blockchain = 'solana'

LEFT OUTER JOIN {{ ref('silver_solana__contract_names') }} AS from_labels
ON e.tx_from_address COLLATE 'en-ci' = from_labels.address AND from_labels.blockchain = 'solana'

