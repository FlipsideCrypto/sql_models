{{ config(
  materialized = 'incremental',
  unique_key = 'tx_id',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'ethereum', 'nft']
) }}

SELECT
  'crypto_kitties' AS event_platform,
  tx_id,
  block_timestamp,
  'mint' AS event_type,
  contract_addr AS contract_address,
  event_inputs :kittyId AS token_id,
  '0x0000000000000000000000000000000000000000' AS event_from,
  event_inputs :owner AS event_to,
  0 AS price,
  0 AS platform_fee,
  0 AS creator_fee,
  'ETH' AS tx_currency
FROM
  {{ ref('silver_ethereum__events_emitted') }}
WHERE
  event_name = 'Birth'
  AND contract_addr = '0x06012c8cf97bead5deae237070f9587f8e7a266d'

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
