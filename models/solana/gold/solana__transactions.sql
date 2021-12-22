{{ config(
    materialized = 'view', 
    tags = ['snowflake', 'solana', 'gold_solana', 'solana_transactions']
) }}

SELECT
    DISTINCT
    block_timestamp, 
    block_id, 
    recent_blockhash, 
    tx_id, 
    pre_mint, 
    post_mint, 
    tx_from_address, 
    from_labels.label_type AS tx_from_label, 
    from_labels.label_subtype AS tx_from_label_subtype, 
    from_labels.program_name as tx_from_program_name,
    from_labels.project_name as tx_from_address_name,
    tx_to_address,
    to_labels.label_type AS tx_to_label, 
    to_labels.label_subtype AS tx_to_label_subtype, 
    to_labels.program_name as tx_to_program_name,
    to_labels.project_name as tx_to_address_name,
    fee, 
    succeeded, 
    program_id,
    contract_labels.project_name AS program_name, 
    ingested_at, 
    transfer_tx_flag
FROM 
    {{ ref('silver_solana__transactions') }} 

LEFT OUTER JOIN {{ ref('silver_solana__contract_names') }} AS contract_labels
ON program_id = contract_labels.address AND contract_labels.blockchain = 'solana'

LEFT OUTER JOIN {{ ref('silver_solana__contract_names') }} AS to_labels
ON tx_to_address = to_labels.address AND to_labels.blockchain = 'solana'

LEFT OUTER JOIN {{ ref('silver_solana__contract_names') }} AS from_labels
ON tx_from_address = from_labels.address AND from_labels.blockchain = 'solana'