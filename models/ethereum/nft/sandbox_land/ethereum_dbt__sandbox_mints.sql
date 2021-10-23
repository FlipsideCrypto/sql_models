{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = 'tx_id',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'ethereum', 'nft']
) }}

SELECT
  'sandboxs_land' AS event_platform,
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
  contract_addr = '0x50f5474724e0ee42d9a4e711ccfb275809fd6d4a'
  AND event_inputs :_from = '0x0000000000000000000000000000000000000000'
  AND

{% if is_incremental() %}
block_timestamp >= getdate() - INTERVAL '1 days'
{% else %}
  block_timestamp >= getdate() - INTERVAL '9 months'
{% endif %}
