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
 'polkamon' as event_platform,
  tx_id, 
  block_timestamp, 
  'mint' AS event_type,
  contract_addr AS contract_address,
  event_inputs:id AS token_id,
  '0x0000000000000000000000000000000000000000' AS event_from,
  event_inputs:owner AS event_to,
  0 AS price,
  0 AS platform_fee, 
  0 AS creator_fee,
  'ETH' AS tx_currency
FROM {{ source('ethereum', 'ethereum_events_emitted') }}

WHERE contract_addr = '0x85f0e02cb992aa1f9f47112f815f519ef1a59e2d'
 AND event_name = 'Transfer'
 
 AND
{% if is_incremental() %}
    block_timestamp >= getdate() - interval '5 days'
{% else %}
    block_timestamp >= getdate() - interval '9 months'
{% endif %}

