{{ config(
  materialized = 'incremental',
  unique_key = 'tx_id || nf_token_id',
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp', 'block_id'],
  tags = ['snowflake', 'uniswapv3_silver', 'silver_uniswapv3__liquidity_actions']
) }}

SELECT
  *
FROM
  {{ ref('uniswapv3_dbt__liquidity_actions') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND system_created_at :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(system_created_at :: DATE))
  FROM
    {{ this }} AS positions
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY nf_token_id, tx_id
ORDER BY
  system_created_at DESC)) = 1
