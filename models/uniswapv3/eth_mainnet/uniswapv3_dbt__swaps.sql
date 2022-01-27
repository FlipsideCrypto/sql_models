{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', tx_id, log_index)",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'uniswapv3_silver', 'uniswapv3_dbt__swaps']
) }}

WITH base_tables AS (

  SELECT
    *
  FROM
    {{ source(
      'bronze',
      'prod_ethereum_sink_407559501'
    ) }}
  WHERE
    record_content :model :name :: STRING IN (
      'uniswap_v3_mainnet_swap_model'
    )

{% if is_incremental() %}
AND (
  record_metadata :CreateTime :: INT / 1000
) :: TIMESTAMP :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(system_created_at :: DATE))
  FROM
    {{ this }}
)
{% endif %}
)
SELECT
  (
    record_metadata :CreateTime :: INT / 1000
  ) :: TIMESTAMP AS system_created_at,
  t.value :amount0 :: FLOAT AS amount0,
  t.value :amount1 :: FLOAT AS amount1,
  t.value :block_id :: INTEGER AS block_id,
  t.value :block_timestamp :: TIMESTAMP AS block_timestamp,
  t.value :blockchain :: STRING AS blockchain,
  t.value :liquidity :: FLOAT AS liquidity,
  t.value :liquidity_adjusted :: FLOAT AS liquidity_adjusted,
  t.value :log_index :: INTEGER AS log_index,
  t.value :pool_address :: STRING AS pool_address,
  t.value :price :: FLOAT AS price,
  t.value :price_0_1 :: FLOAT AS price_0_1,
  t.value :price_1_0 :: FLOAT AS price_1_0,
  t.value :recipient :: STRING AS recipient,
  t.value :sender :: STRING AS sender,
  t.value :sqrt_price_x96 :: FLOAT AS sqrt_price_x96,
  t.value :tick :: INTEGER AS tick,
  t.value :tx_id :: STRING AS tx_id
FROM
  base_tables,
  LATERAL FLATTEN(
    input => record_content :results
  ) t
