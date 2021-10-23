{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = 'tx_id',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'ethereum', 'nft']
) }}

SELECT
  'hashmasks' AS event_platform,
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
  contract_address = '0xc2c747e0f7004f9e8817db2ca4997657a7746928'
  AND event_name = 'Transfer'
  AND event_inputs :from = '0x0000000000000000000000000000000000000000'
  AND

{% if is_incremental() %}
block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
