{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', tx_id, msg_index, action_index)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra_silver', 'event_actions']
) }}


WITH silver_terra_raw AS (
  SELECT
    *
  FROM
  {{ ref('silver_terra__msg_events') }}

{% if is_incremental() %}
  WHERE block_timestamp >= getdate() - INTERVAL '1 days'
  {% endif %}

),

silver_terra_raw_lateral AS (
  SELECT 
    system_created_at,
    block_id,
    block_timestamp,
    blockchain,
    chain_id,
    tx_id,
    tx_status,
    tx_module,
    tx_type,
    msg_index,
    msg_module,
    msg_type,
    event_index,
    event_type,
    event_attributes,
    event_attributes_array_index,  
    object_insert(
      object_agg(
        event_attributes_array_key, event_attributes_array_values
      ),
      'array_index', event_attributes_array_index
    ) AS event_attributes_array
  FROM (
    SELECT
      system_created_at,
      block_id,
      block_timestamp,
      blockchain,
      chain_id,
      tx_id,
      tx_status,
      tx_module,
      tx_type,
      msg_index,
      msg_module,
      msg_type,
      event_index,
      event_type,
      event_attributes,
      CASE 
        WHEN try_to_decimal(TO_CHAR(split(f.key, '_' )[0])) IS NOT NULL 
        THEN TO_CHAR(split(f.key, '_' )[0])
        ELSE '0'
      END AS event_attributes_array_index,
      CASE 
        WHEN try_to_decimal(TO_CHAR(split(f.key, '_' )[0])) IS NOT NULL 
        THEN array_to_string(array_slice(split(f.key, '_' ), 1, array_size(split(f.key, '_' ))), '_') 
        ELSE array_to_string(array_slice(split(f.key, '_' ), 0, array_size(split(f.key, '_' ))), '_')
      END AS event_attributes_array_key,
      f.value AS event_attributes_array_values
    FROM silver_terra_raw,
    lateral flatten(input => event_attributes) AS f
  )
  GROUP BY 
    system_created_at,
    block_id,
    block_timestamp,
    blockchain,
    chain_id,
    tx_id,
    tx_status,
    tx_module,
    tx_type,
    msg_index,
    msg_module,
    msg_type,
    event_index,
    event_type,
    event_attributes,
    event_attributes_array_index
),

silver_terra_arr AS (
SELECT 
  system_created_at,
  block_id,
  block_timestamp,
  blockchain,
  chain_id,
  tx_id,
  tx_status,
  tx_module,
  tx_type,
  msg_index,
  msg_module,
  msg_type,
  event_index,
  event_type,
  event_attributes,
  array_agg(event_attributes_array) WITHIN group (ORDER BY event_attributes_array_index ASC) AS event_attributes_array
FROM silver_terra_raw_lateral
GROUP BY 
  system_created_at,
  block_id,
  block_timestamp,
  blockchain,
  chain_id,
  tx_id,
  tx_status,
  tx_module,
  tx_type,
  msg_index,
  msg_module,
  msg_type,
  event_index,
  event_type,
  event_attributes
),

msgs AS (
SELECT
tx_id,
msg_index,
msg_type,
em.key AS parsed_msg
FROM {{ ref('silver_terra__msgs') }} m,
  lateral flatten(m.msg_value :execute_msg) em

{% if is_incremental() %}
WHERE block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
),

msg_events AS (

SELECT
tx_id,
block_timestamp,
msg_index,
a.value:contract_address::STRING AS contract_address,
coalesce(a.value:action::STRING, a.value:method::STRING) AS event_action,
a.value:array_index::STRING AS action_index,
a.value::OBJECT AS from_contract_logs
FROM silver_terra_arr r,
lateral flatten(input => r.event_attributes_array) a
WHERE r.event_type = 'from_contract'
)

SELECT
e.tx_id,
e.block_timestamp,
e.msg_index,
contract_address,
coalesce(event_action, m.parsed_msg) AS parsed_action,
action_index::NUMERIC AS action_index,
from_contract_logs
FROM msg_events e
LEFT JOIN msgs m
ON e.tx_id = m.tx_id AND e.msg_index = m.msg_index
where msg_type not in ('wasm/MsgInstantiateContract', 'wasm/MsgMigrateContract')