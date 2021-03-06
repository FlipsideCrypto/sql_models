{{ config(
  materialized = 'incremental',
  unique_key = 'tx_id',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'ethereum', 'nft']
) }}

SELECT
  'makersplace' AS event_platform,
  tx_id,
  block_timestamp,
  'mint' AS event_type,
  contract_addr AS contract_address,
  event_inputs :id AS token_id,
  '0x0000000000000000000000000000000000000000' AS event_from,
  event_inputs :owner AS event_to,
  0 AS price,
  0 AS platform_fee,
  0 AS creator_fee,
  'ETH' AS tx_currency
FROM
  {{ ref('silver_ethereum__events_emitted') }}
WHERE
  contract_addr = '0x2a46f2ffd99e19a89476e2f62270e0a35bbf0756'
  AND event_name = 'DigitalMediaReleaseCreateEvent'

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
