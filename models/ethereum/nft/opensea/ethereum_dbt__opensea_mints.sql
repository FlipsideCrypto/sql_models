{{ config(
  materialized = 'incremental',
  unique_key = 'tx_id',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'ethereum', 'nft']
) }}

SELECT
  'opensea_nfts' AS event_platform,
  tx_id,
  block_timestamp,
  'mint' AS event_type,
  contract_addr AS contract_address,
  event_inputs :tokenId AS token_id,
  '0x0000000000000000000000000000000000000000' AS event_from,
  event_inputs :to AS event_to,
  0 AS price,
  0 AS platform_fee,
  0 AS creator_fee,
  'ETH' AS tx_currency
FROM
  {{ ref('silver_ethereum__events_emitted') }}
WHERE
  tx_id IN (
    SELECT
      tx_hash
    FROM
      {{ ref('silver_ethereum__events') }}
    WHERE
      input_method = '0x755edd17'
      AND block_timestamp > '2020-01-01'
  )
  AND

{% if is_incremental() %}
block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
