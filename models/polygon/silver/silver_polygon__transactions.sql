{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', chain_id, block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp', 'block_id'],
  tags = ['snowflake', 'polygon_silver', 'polygon_transactions','polygon']
) }}

SELECT
  *
FROM
  {{ ref('polygon_dbt__transactions') }}
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

qualify(ROW_NUMBER() over(PARTITION BY chain_id, tx_id
ORDER BY
  block_id DESC, system_created_at DESC)) = 1
