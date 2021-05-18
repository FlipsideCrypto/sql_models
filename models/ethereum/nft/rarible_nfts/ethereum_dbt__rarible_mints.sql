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
  'rarible_nfts' as event_platform,
  tx_id, 
  block_timestamp, 
  'mint' AS event_type,
  contract_addr AS contract_address,
  event_inputs:_id AS token_id,
  '0x0000000000000000000000000000000000000000' AS event_from,
  event_inputs:_to AS event_to,
  0 AS price,
  0 AS platform_fee, 
  0 AS creator_fee,
  'ETH' AS tx_currency
FROM {{ source('ethereum', 'ethereum_events_emitted') }}

WHERE contract_addr IN (SELECT distinct(contract_address) AS address
                        FROM {{ source('ethereum', 'ethereum_events') }} 
                        WHERE input_method = '0xc6bf3262')
 
 AND event_inputs:_from = '0x0000000000000000000000000000000000000000'
 AND event_name = 'TransferSingle'
 
 AND 
{% if is_incremental() %}
    block_timestamp >= getdate() - interval '1 days'
{% else %}
    block_timestamp >= getdate() - interval '9 months'
{% endif %}