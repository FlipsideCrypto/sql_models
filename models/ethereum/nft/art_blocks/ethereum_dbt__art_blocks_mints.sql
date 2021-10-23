{{ config(
  materialized = 'incremental',
  unique_key = 'tx_id',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'ethereum', 'nft']
) }}

SELECT
  'art_blocks' AS event_platform,
  tx_id,
  block_timestamp,
  'mint' AS event_type,
  contract_addr AS contract_address,
  event_inputs :_tokenId AS token_id,
  '0x0000000000000000000000000000000000000000' AS event_from,
  event_inputs :_to AS event_to,
  0 AS price,
  0 AS platform_fee,
  0 AS creator_fee,
  'ETH' AS tx_currency
FROM
  {{ ref('silver_ethereum__events_emitted') }}
WHERE
  event_name = 'Mint'
  AND contract_addr IN (
    '0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270',
    '0x059edd72cd353df5106d2b9cc5ab83a52287ac3a'
  )
  

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
