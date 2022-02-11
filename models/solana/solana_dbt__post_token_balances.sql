{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id, account_index)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['ingested_at::DATE'],
  tags = ['snowflake', 'solana', 'silver_solana', 'solana_post_token_balances']
) }}

SELECT 
    block_timestamp, 
    block_id, 
    tx_id, 
    b.value :accountIndex :: INTEGER as account_index,
    b.value :mint :: STRING as mint, 
    b.value :owner :: STRING as owner, 
    b.value :uiTokenAmount:amount :: INTEGER as amount, 
    b.value :uiTokenAmount:decimals as decimal, 
    b.value :uiTokenAmount:uiAmount as uiAmount, 
    b.value :uiTokenAmount:uiAmountString as uiAmountString, 
    ingested_at
FROM {{ ref('bronze_solana__transactions') }} t, 
table(flatten(tx:meta:postTokenBalances)) b

{% if is_incremental() %}
  WHERE ingested_at >= getdate() - interval '2 days'
{% endif %}