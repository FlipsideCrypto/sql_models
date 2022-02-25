{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'solana', 'silver_solana', 'solana_transactions']
) }}

SELECT
    block_timestamp :: TIMESTAMP AS block_timestamp,
    block_id :: INTEGER AS block_id,
    chain_id :: STRING AS chain_id, 
    tx :transaction :message :recentBlockhash :: STRING AS recent_block_hash,
    tx_id :: STRING AS tx_id,
    tx :meta :preTokenBalances [0] :mint :: STRING AS pre_mint,
    tx :meta :postTokenBalances [1] :mint :: STRING AS post_mint,
    COALESCE(
        tx :meta :preTokenBalances [0] :owner :: STRING,
        tx: TRANSACTION :message :instructions [0] :parsed :info :source :: STRING
    ) AS tx_from_address,
    COALESCE (
        tx :meta :postTokenBalances [2] :owner :: STRING,
        tx: TRANSACTION :message :instructions [0] :parsed :info :destination :: STRING
    ) AS tx_to_address,
    tx :meta :fee :: INTEGER AS fee,
    CASE WHEN tx :meta :status :Err IS NULL THEN TRUE
    ELSE FALSE
    END AS succeeded,
    tx :transaction :message :instructions [0] :programId :: STRING AS program_id,
    tx :transaction :message :accountKeys :: ARRAY AS account_keys,
    ingested_at :: TIMESTAMP AS ingested_at,
    CASE WHEN len(tx :meta :postTokenBalances [0] ) > 0
    AND len(tx :meta :preTokenBalances [0]) > 0 THEN TRUE
    ELSE FALSE
    END AS transfer_tx_flag

FROM {{ ref('bronze_solana__transactions') }}

WHERE block_timestamp :: date >= '2022-02-22' 
AND program_id <> 'Vote111111111111111111111111111111111111111'
AND program_id <> 'FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH'

{% if is_incremental() %}
AND ingested_at >= getdate() - INTERVAL '2 days'
{% endif %}