{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', tx_id, action_index, msg_index)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra_silver', 'event_actions']
) }}


  SELECT DISTINCT
    blockchain, 
    block_id, 
    block_timestamp, 
    chain_id, 
    tx_id, 
    SPLIT(key, '_')[0]::INTEGER AS action_index, 
    msg_index,
    value:contract_address::STRING AS action_contract_address, 
    COALESCE(value:action_log:action::STRING, value:action_log:method::STRING) AS action_method, 
    object_delete(value:action_log, 'action', 'method') AS action_log
  FROM {{ ref('terra_dbt__msg_events_actions') }} r,
  lateral flatten(input => r.event_attributes_actions)

  {% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp)
    FROM
      {{ ref('terra_dbt__msg_events_actions') }}
  )
{% endif %}

  qualify(RANK() over(PARTITION BY tx_id
  ORDER BY
    block_id DESC)) = 1

