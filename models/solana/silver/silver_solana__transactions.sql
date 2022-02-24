{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'solana', 'silver_solana', 'solana_transactions']
) }}

SELECT
  block_timestamp,
  block_id,
  recent_block_hash,
  tx_id,
  pre_mint,
  post_mint,
  tx_from_address,
  tx_to_address,
  fee,
  succeeded,
  program_id,
  ingested_at,
  transfer_tx_flag,
  account_keys

FROM {{ ref('solana_dbt__transactions') }}
  
{% if is_incremental() %}
  WHERE ingested_at >= getdate() - INTERVAL '2 days'
{% endif %}

QUALIFY(ROW_NUMBER() over(PARTITION BY block_id, tx_id
ORDER BY
  ingested_at DESC)) = 1
