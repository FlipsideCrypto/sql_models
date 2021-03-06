{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  persist_docs={"relation": true, "columns": true}, 
  tags = ['snowflake', 'solana', 'silver_solana', 'solana_votes']
) }}

WITH base_table AS (

  SELECT
    block_timestamp :: TIMESTAMP AS block_timestamp,
    block_id :: INTEGER AS block_id,
    chain_id :: STRING AS blockchain,
    tx :transaction :message :recentBlockhash :: STRING AS recent_block_hash,
    tx_id :: STRING AS tx_id,
    CASE
      WHEN tx :meta :status :Err IS NULL THEN TRUE
      ELSE FALSE
    END AS succeeded,
    tx :transaction :message :instructions [0] :program :: STRING AS program_type,
    tx :transaction :message :instructions [0] :programId :: STRING AS program_id,
    tx :transaction :message :instructions [0] :parsed :info :voteAccount :: STRING AS vote_account,
    tx :transaction :message :instructions [0] :parsed :info :voteAuthority :: STRING AS vote_authority,
    ingested_at :: TIMESTAMP AS ingested_at,
    CASE
      WHEN len(
        tx :meta :postTokenBalances [0]
      ) > 0
      AND len(
        tx :meta :preTokenBalances [0]
      ) > 0
      AND succeeded = TRUE THEN TRUE
      ELSE FALSE
    END AS transfer_tx_flag
  FROM
    {{ ref('bronze_solana__transactions') }}
  WHERE
    tx :transaction :message :instructions [0] :parsed :type :: STRING IS NOT NULL
    AND tx :transaction :message :instructions [0] :programId :: STRING = 'Vote111111111111111111111111111111111111111'

{% if is_incremental() %}
  AND ingested_at >= getdate() - interval '2 days'
{% endif %}

)
SELECT
  block_timestamp,
  block_id,
  blockchain,
  recent_block_hash,
  tx_id,
  succeeded,
  program_type,
  program_id,
  vote_account,
  vote_authority,
  ingested_at,
  transfer_tx_flag
FROM
  base_table qualify(ROW_NUMBER() over(PARTITION BY block_id, tx_id
ORDER BY
  ingested_at DESC)) = 1
