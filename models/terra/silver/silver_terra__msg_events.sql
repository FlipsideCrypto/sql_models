{{ config(
  materialized = 'incremental',
  unique_key = 'chain_id || block_id || tx_id || msg_index || event_index || event_type',
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp', 'block_id', 'tx_id'],
  tags = ['snowflake', 'terra_silver', 'terra_msg_events']
) }}

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
    silver_terra.msg_events
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY chain_id, block_id, tx_id, msg_index, event_index, event_type
ORDER BY
  system_created_at DESC)) = 1
