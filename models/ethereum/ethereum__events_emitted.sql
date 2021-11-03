{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='block_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp', 'contract_address'],
    tags=['snowflake', 'ethereum', 'events_emitted']
  )
}}

SELECT 
  DISTINCT
  BLOCK_ID AS block_id,
  BLOCK_TIMESTAMP AS block_timestamp,
  TX_ID AS tx_id,
  EVENT_INDEX AS event_index,
  EVENT_INPUTS AS event_inputs,
  EVENT_NAME AS event_name,
  EVENT_REMOVED AS event_removed,
  TX_FROM_ADDR AS tx_from_address,
  from_labels.l1_label as tx_from_label_type,
  from_labels.l2_label as tx_from_label_subtype,
  from_labels.address as tx_from_label,
  from_labels.project_name as tx_from_address_name,
  TX_TO_ADDR AS tx_to_address,
  to_labels.l1_label as tx_to_label_type,
  to_labels.l2_label as tx_to_label_subtype,
  to_labels.address as tx_to_label,
  to_labels.project_name as tx_to_address_name,
  CONTRACT_ADDR AS contract_address,
  COALESCE(contract_labels.address,CONTRACT_NAME) AS contract_name,
  TX_SUCCEEDED AS tx_succeeded
FROM {{ ref('silver_ethereum__events_emitted') }} b

LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} as from_labels
 ON b.TX_FROM_ADDR = from_labels.address
 
LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} as to_labels
 ON b.TX_TO_ADDR = to_labels.address
 
LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} as contract_labels
 ON b.CONTRACT_ADDR = contract_labels.address

WHERE 1=1
{% if is_incremental() %}
AND  b.block_timestamp >= getdate() - interval '40 hours'
{% endif %}