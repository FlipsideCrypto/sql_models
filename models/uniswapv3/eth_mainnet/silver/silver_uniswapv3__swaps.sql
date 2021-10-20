{{ config(
  materialized = 'incremental',
  unique_key = 'tx_id || log_index',
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp', 'block_id'],
  tags = ['snowflake', 'uniswapv3_silver', 'silver_uniswapv3__swaps']
) }}

SELECT
  *
FROM
  {{ ref('uniswapv3_dbt__swaps') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND system_created_at :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(system_created_at :: DATE))
  FROM
    {{ this }} AS swaps
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY tx_id, log_index
ORDER BY
  system_created_at DESC)) = 1
