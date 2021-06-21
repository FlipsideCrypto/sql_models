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

WITH prices as (
  SELECT 
      date_trunc('hour', block_timestamp) as hour,
      currency,
      symbol,
      avg(price_usd) as price_usd
    FROM {{ ref('terra__oracle_prices')}} 
    WHERE
    {% if is_incremental() %}
      block_timestamp >= getdate() - interval '1 days'
    {% else %}
      block_timestamp >= getdate() - interval '9 months'
    {% endif %} 
    GROUP BY 1,2,3
)

SELECT 
  t.blockchain,
  t.chain_id,
  t.tx_status,
  t.block_id,
  t.block_timestamp, 
  t.tx_id, 
  t.msg_type, 
  REGEXP_REPLACE(msg_value:proposer,'\"','') as proposer,
  proposer_labels.l1_label as proposer_label_type,
  proposer_labels.l2_label as proposer_label_subtype,
  proposer_labels.project_name as proposer_address_label,
  proposer_labels.address_name as proposer_address_name,
  REGEXP_REPLACE(msg_value:content:value:type,'\"','') as proposal_type,
  REGEXP_REPLACE(msg_value:content:value:description,'\"','') as description,
  REGEXP_REPLACE(msg_value:content:value:title,'\"','') as title,
  -- REGEXP_REPLACE(msg_value:initial_deposit[0]:amount,'\"','') as deposit_amount,
  msg_value:initial_deposit[0]:amount as deposit_amount,
  deposit_amount * o.price_usd as deposit_amount_usd,
  REGEXP_REPLACE(msg_value:initial_deposit[0]:denom,'\"','') as deposit_currency
FROM {{source('silver_terra', 'msgs')}} t

LEFT OUTER JOIN prices o
 ON date_trunc('hour', t.block_timestamp) = o.hour
 AND REGEXP_REPLACE(t.msg_value:initial_deposit[0]:denom,'\"','') = o.currency 

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} as proposer_labels
ON REGEXP_REPLACE(t.msg_value:proposer,'\"','') = proposer_labels.address

WHERE msg_module = 'gov' 
  AND msg_type = 'gov/MsgSubmitProposal'
  AND tx_status = 'SUCCEEDED'

{% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
{% else %}
 AND block_timestamp >= getdate() - interval '9 months'
{% endif %}