{{ config(
  materialized = 'incremental',
  unique_key = 'pool_address || block_id',
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp', 'block_id'],
  tags = ['snowflake', 'uniswapv3_silver', 'silver_uniswapv3__pool_stats']
) }}

SELECT
  *
FROM
  {{ ref('uniswapv3_dbt__pool_stats') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND system_created_at :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(system_created_at :: DATE))
  FROM
    {{ this }} AS pool_stats
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY pool_address, block_id
ORDER BY
  system_created_at DESC)) = 1
