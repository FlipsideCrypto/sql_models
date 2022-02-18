{{ config(
  materialized = 'incremental',
  unique_key = 'tx_id',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'ethereum', 'nft']
) }}

WITH auction_cancelled AS (

  SELECT
    tx_id,
    block_timestamp
  FROM
    {{ ref('silver_ethereum__events_emitted') }}
  WHERE
    event_name = 'AuctionCancelled'
    AND contract_addr = '0xb1690c08e213a35ed9bab7b318de14420fb57d8c'
)
SELECT
  'crypto_kitties' AS event_platform,
  eee.tx_id,
  eee.block_timestamp,
  'unlist' AS event_type,
  contract_addr AS contract_address,
  event_inputs :tokenId AS token_id,
  event_inputs :from AS event_from,
  event_inputs :to AS event_to,
  0 AS price,
  0 AS platform_fee,
  0 AS creator_fee,
  'ETH' AS tx_currency
FROM
  {{ ref('silver_ethereum__events_emitted') }}
  eee
  INNER JOIN auction_cancelled
  ON eee.tx_id = auction_cancelled.tx_id
WHERE
  event_name = 'Transfer'
  AND contract_addr = '0x06012c8cf97bead5deae237070f9587f8e7a266d'

{% if is_incremental() %}
AND eee.block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
