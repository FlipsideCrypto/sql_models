{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='block_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp'],
    tags=['snowflake', 'terra', 'gov']
  )
}}

WITH balances as (
  SELECT 
  date,
  address, 
  balance
  FROM {{source('terra', 'udm_daily_balances_terra')}}
  WHERE balance_type = 'staked'

{% if is_incremental() %}
 AND date >= getdate() - interval '1 days'
{% else %}
 AND date >= getdate() - interval '9 months'
{% endif %}
)

SELECT 
  a.blockchain,
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
  REGEXP_REPLACE(msg_value:option,'\"','') as "option",
  b.balance
FROM {{source('silver_terra', 'msgs')}} a

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} as voter_labels
 ON msg_value:voter = voter_labels.address

LEFT OUTER JOIN balances b 
 ON date(a.block_timestamp) = date(b.date)

WHERE msg_module = 'gov' 
  AND msg_type = 'gov/MsgVote'
  AND tx_status = 'SUCCEEDED'

{% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
{% else %}
 AND block_timestamp >= getdate() - interval '9 months'
{% endif %}