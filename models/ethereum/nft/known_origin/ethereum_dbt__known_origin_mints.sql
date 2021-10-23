{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = 'tx_id',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'ethereum', 'nft']
) }}

SELECT
  'known_origin' AS event_platform,
  tx_id,
  block_timestamp,
  'mint' AS event_type,
  contract_addr AS contract_address,
  event_inputs :_tokenId AS token_id,
  '0x0000000000000000000000000000000000000000' AS event_from,
  event_inputs :_buyer AS event_to,
  0 AS price,
  0 AS platform_fee,
  0 AS creator_fee,
  'ETH' AS tx_currency
FROM
  {{ ref('silver_ethereum__events_emitted') }}
WHERE
  contract_addr = '0xfbeef911dc5821886e1dda71586d90ed28174b7d'
  AND event_name = 'Minted'
  AND

{% if is_incremental() %}
block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
