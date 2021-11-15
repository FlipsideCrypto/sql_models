{{ 
  config(
    materialized='incremental',
    unique_key='block_id || tx_id || event_index',
    incremental_strategy='delete+insert',
    cluster_by=['block_id','block_timestamp'],
    tags=['snowflake', 'polygon', 'polygon_events_emitted_gold']
  )
}}

with poly_labels AS (
    SELECT
        l1_label,
        l2_label,
        project_name,
        address_name, 
        address
    FROM
        {{ ref('silver_crosschain__address_labels') }}
    WHERE
        blockchain = 'polygon'
)
SELECT 
  BLOCK_ID AS block_id,
  BLOCK_TIMESTAMP AS block_timestamp,
  TX_ID AS tx_id,
  EVENT_INDEX AS event_index,
  EVENT_INPUTS AS event_inputs,
  EVENT_NAME AS event_name,
  EVENT_REMOVED AS event_removed,
  TX_FROM AS tx_from_address,
  from_labels.l1_label as tx_from_label_type,
  from_labels.l2_label as tx_from_label_subtype,
  from_labels.project_name as tx_from_label,
  from_labels.address_name as tx_from_address_name,
  TX_TO AS tx_to_address,
  to_labels.l1_label as tx_to_label_type,
  to_labels.l2_label as tx_to_label_subtype,
  to_labels.project_name as tx_to_label,
  to_labels.address_name as tx_to_address_name,
  CONTRACT_ADDRESS AS contract_address,
  COALESCE(contract_labels.address,CONTRACT_NAME) AS contract_name,
  TX_SUCCEEDED AS tx_succeeded
FROM {{ ref('silver_polygon__events_emitted')}} b

LEFT OUTER JOIN poly_labels as from_labels
 ON b.TX_FROM = from_labels.address
 
LEFT OUTER JOIN poly_labels as to_labels
 ON b.TX_TO = to_labels.address
 
LEFT OUTER JOIN poly_labels as contract_labels
 ON b.CONTRACT_ADDRESS = contract_labels.address

WHERE 1=1
{% if is_incremental() %}
 AND b.block_timestamp::date >= (select max(block_timestamp::date) from {{ this }})
{% endif %}