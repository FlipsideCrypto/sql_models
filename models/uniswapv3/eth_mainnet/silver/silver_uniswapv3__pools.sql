{{ config(
  materialized = 'incremental',
  unique_key = 'pool_address',
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp', 'block_id'],
  tags = ['snowflake', 'uniswapv3_silver', 'silver_uniswapv3__pools']
) }}

SELECT
  *
FROM
  {{ ref('uniswapv3_dbt__pools') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND system_created_at :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(system_created_at :: DATE))
  FROM
    {{ this }} AS pools
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY pool_address
ORDER BY
  system_created_at DESC)) = 1
