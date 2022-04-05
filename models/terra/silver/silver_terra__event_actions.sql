{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', tx_id, action_index)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra_silver', 'event_actions']
) }}


WITH raw_table AS (
  SELECT 
    *
  FROM {{ ref('terra_dbt__msg_events_actions') }} 
),

clean_table AS (
  SELECT DISTINCT
    blockchain, 
    block_id, 
    block_timestamp, 
    chain_id, 
    tx_id, 
    SPLIT(key, '_')[0]::INTEGER AS action_index, 
    value:contract_address::STRING AS action_contract_address, 
    COALESCE(value:action_log:action::STRING, value:action_log:method::STRING) AS action_method, 
    object_delete(value:action_log, 'action', 'method') AS action_log
  FROM raw_table r,
  lateral flatten(input => r.event_attributes_actions)
)

SELECT * FROM clean_table