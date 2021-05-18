{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='tx_id', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'ethereum', 'nft']
  )
}}

SELECT 
  'niftygateway' AS event_platform,
  tx_id, 
  block_timestamp, 
  'mint' AS event_type,
  contract_addr AS contract_address,
  event_inputs:_tokenId AS token_id,
  '0x0000000000000000000000000000000000000000' AS event_from,
  event_inputs:new_owner AS event_to,
  0 AS price,
  0 AS platform_fee, 
  0 AS creator_fee,
  'ETH' AS tx_currency
FROM {{ source('ethereum', 'ethereum_events_emitted') }}
WHERE event_name = 'NiftyCreated'
AND {% if is_incremental() %}
    block_timestamp >= getdate() - interval '1 days'
{% else %}
    block_timestamp >= getdate() - interval '9 months'
{% endif %}