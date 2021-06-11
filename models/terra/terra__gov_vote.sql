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
  REGEXP_REPLACE(msg_value:voter,'\"','') as voter,
  voter_labels.l1_label as voter_label_type,
  voter_labels.l2_label as voter_label_subtype,
  voter_labels.project_name as voter_address_label,
  voter_labels.address_name as voter_address_name,
  REGEXP_REPLACE(msg_value:proposal_id,'\"','') as proposal_id,
  REGEXP_REPLACE(msg_value:option,'\"','') as "option"
FROM {{source('terra', 'terra_msgs')}} 

LEFT OUTER JOIN {{source('shared','udm_address_labels')}} as voter_labels
ON msg_value:voter = voter_labels.address

WHERE msg_module = 'gov' 
  AND msg_type = 'gov/MsgVote'

{% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
{% else %}
 AND block_timestamp >= getdate() - interval '9 months'
{% endif %}