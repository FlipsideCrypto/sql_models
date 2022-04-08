{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', chain_id, block_id, tx_id, msg_index, event_index, event_type)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra_silver', 'terra_msg_events'],
  post_hook = '{{ delete_uncle_block_tx() }}'
) }}

WITH msg_events_uncle_blocks_removed AS (

  SELECT
    *
  FROM
    {{ ref('terra_dbt__msg_events') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp)
    FROM
      {{ this }}
  )
{% endif %}

qualify(RANK() over(PARTITION BY tx_id
ORDER BY
  block_id DESC)) = 1
)

SELECT
  *
FROM
  msg_events_uncle_blocks_removed qualify(ROW_NUMBER() over(PARTITION BY chain_id, block_id, tx_id, msg_index, event_index, event_type
ORDER BY
  system_created_at DESC)) = 1
