{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='block_timestamp',
    incremental_strategy='delete+insert',
    tags=['snowflake', 'flow', 'events']
  )
}}

SELECT
  d.block_id,
  d.block_timestamp,
  t.tx_id,
  t.tx_to as delegator_address,
  d.event_from as delegator_id,
  d.event_to as node_id
FROM {{ source('flow', 'udm_events_flow')}} d

JOIN {{ source('flow', 'udm_events_flow')}} t
  ON t.tx_id = d.tx_id

JOIN {{ source('flow', 'udm_events_flow')}} n
  ON t.tx_id = n.tx_id

WHERE 
  d.event_type = 'delegator_tokens_committed'
  AND n.event_type = 'new_delegator_created'
  AND t.tx_to is not null

  {% if is_incremental() %}
  AND d.block_timestamp >= getdate() - interval '7 days'
  {% else %}
  AND d.block_timestamp >= getdate() - interval '9 months'
  {% endif %}


UNION


SELECT
  t.block_id,
  t.block_timestamp,
  t.tx_id,
  t.event_from as delegator_address,
  '0' as delegator_id,
  t.event_to as node_id
FROM {{ source('flow', 'udm_events_flow')}} t
WHERE 
  t.event_type = 'new_node_created'

  {% if is_incremental() %}
  AND t.block_timestamp >= getdate() - interval '7 days'
  {% else %}
  AND t.block_timestamp >= getdate() - interval '9 months'
  {% endif %}
