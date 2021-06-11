{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra', 'gov']
  )
}}

SELECT 
  blockchain,
  chain_id,
  tx_status,
  block_id,
  block_timestamp, 
  tx_id, 
  msg_type, 
  REGEXP_REPLACE(msg_value:proposer,'\"','') as proposer,
  REGEXP_REPLACE(msg_value:content:value:type,'\"','') as proposal_type,
  REGEXP_REPLACE(msg_value:content:value:description,'\"','') as description,
  REGEXP_REPLACE(msg_value:content:value:title,'\"','') as title,
  REGEXP_REPLACE(msg_value:initial_deposit[0]:amount,'\"','') as deposit_amount,
  REGEXP_REPLACE(msg_value:initial_deposit[0]:denom,'\"','') as deposit_currency,
  msg_value
FROM {{source('terra', 'terra_msgs')}}  
WHERE msg_module = 'gov' 
  AND msg_type = 'gov/MsgSubmitProposal'

{% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
{% else %}
 AND block_timestamp >= getdate() - interval '9 months'
{% endif %}