{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='block_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp'],
    tags=['snowflake', 'terra', 'labels']
  )
}}

WITH validators as (
SELECT 
  msg_value:pubkey::string as pubkey,
  msg_value:validator_address::string as validator_address
FROM {{source('silver_terra', 'msgs')}}
WHERE msg_module = 'staking' 
  AND msg_type = 'staking/MsgCreateValidator'

{% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
{% else %}
 AND block_timestamp >= getdate() - interval '9 months'
{% endif %}
)

SELECT 
  vp.address as voting_power_address,
  v.validator_address, 
  vp.public_key,
  l.project_name as label
FROM  {{source('terra', 'terra_validator_voting_power')}} vp

JOIN validators v
  ON vp.public_key = v.pubkey
  
LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} l
  ON v.validator_address = l.address

WHERE
{% if is_incremental() %}
  block_timestamp >= getdate() - interval '1 days'
{% else %}
  block_timestamp >= getdate() - interval '9 months'
{% endif %}

GROUP BY 1,2,3,4