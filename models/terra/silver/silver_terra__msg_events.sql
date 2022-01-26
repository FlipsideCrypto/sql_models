{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', chain_id, block_id, tx_id, msg_index, event_index, event_type)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra_silver', 'terra_msg_events']
) }}

WITH silver_terra_raw AS (
  SELECT
    *
  FROM
    {{ ref('terra_dbt__msg_events') }}
  WHERE
    1 = 1

  {% if is_incremental() %}
  AND system_created_at :: DATE >= (
    SELECT
      DATEADD('day', -1, MAX(system_created_at :: DATE))
    FROM
      {{ this }} AS msg_events
  )
  {% endif %}

  qualify(ROW_NUMBER() over(PARTITION BY chain_id, block_id, tx_id, msg_index, event_index, event_type
  ORDER BY
    system_created_at DESC)) = 1
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
    lateral flatten(input => event_attributes) as f
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
)

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
  array_agg(event_attributes_array) within group (order by event_attributes_array_index asc) AS event_attributes_array
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



