{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='tx_id', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'ethereum', 'nft']
  )
}}

WITH mints AS (
SELECT 
  'eulerbeats' AS event_platform, 
  tx_id, 
  block_timestamp,
  'print_minted' AS event_type,
  'eulerbeats' AS project_name,
  contract_address,
  event_inputs:seed AS token_id,
  '0x0000000000000000000000000000000000000000' AS event_from,
  event_inputs:to AS event_to,
  event_inputs:royaltyPaid / power(10, 18) AS creator_fee
FROM {{ ref('ethereum__events_emitted') }}

WHERE contract_address IN ('0x8754f54074400ce745a7ceddc928fb1b7e985ed6', 
                           '0xa98771a46dcb34b34cdad5355718f8a97c8e603e')
AND event_name = 'PrintMinted'
)

SELECT
  event_platform,
  eue.tx_id,
  eue.block_timestamp,
  mints.event_type,
  mints.contract_address,
  project_name,
  token_id,
  event_from,
  event_to,
  amount AS price,
  --price_usd added later
  0 AS platform_fee,
  creator_fee,
  'ETH' AS tx_currency
FROM {{ ref('ethereum__udm_events') }} eue

JOIN mints 
  ON  eue.tx_id = mints.tx_id 
  AND eue.from_address = mints.event_to

WHERE eue.block_timestamp >= (SELECT min(block_timestamp) 
                              FROM mints)

AND eue.tx_id IN (SELECT tx_id 
                  FROM mints)

AND symbol = 'ETH'