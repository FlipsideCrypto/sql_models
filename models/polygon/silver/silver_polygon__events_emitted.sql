{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', chain_id, block_id, tx_id, event_index)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp', 'block_id'],
  tags = ['snowflake', 'polygon_silver', 'polygon_events_emitted','polygon']
) }}

SELECT
  *
FROM
  (
    SELECT
      *
    FROM
      {{ ref('polygon_dbt__events_emitted') }}
    WHERE
      1 = 1

{% if is_incremental() %}
AND system_created_at >= (
  SELECT
    DATEADD('day', -1, MAX(system_created_at :: DATE))
  FROM
    {{ this }}
)
{% endif %}

qualify(RANK() over(PARTITION BY tx_id
ORDER BY
  block_id DESC)) = 1
) A qualify(ROW_NUMBER() over(PARTITION BY chain_id, block_id, tx_id, event_index
ORDER BY
  system_created_at DESC)) = 1
