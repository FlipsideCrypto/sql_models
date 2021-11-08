{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', chain_id, block_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra_silver', 'terra_blocks']
) }}

SELECT
  *
FROM
  {{ ref('terra_dbt__blocks') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND system_created_at :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(system_created_at :: DATE))
  FROM
    {{ this }} AS blocks
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY chain_id, block_id
ORDER BY
  system_created_at DESC)) = 1
