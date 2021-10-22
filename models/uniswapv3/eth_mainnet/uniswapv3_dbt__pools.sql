{{ config(
  materialized = 'incremental',
  unique_key = 'pool_address',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'uniswapv3_silver', 'uniswapv3_dbt__pools']
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
      'uniswap_v3_mainnet_pool_model'
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
  t.value :block_id :: INTEGER AS block_id,
  t.value :block_timestamp :: TIMESTAMP AS block_timestamp,
  t.value :blockchain :: STRING AS blockchain,
  t.value :factory_address :: STRING AS factory_address,
  t.value :fee :: FLOAT AS fee,
  t.value :init_price :: FLOAT AS init_price,
  t.value :init_sqrt_price_x96 :: FLOAT AS init_sqrt_price_x96,
  t.value :init_tick :: INTEGER AS init_tick,
  t.value :pool_address :: STRING AS pool_address,
  t.value :pool_name :: STRING AS pool_name,
  t.value :tick_spacing :: INTEGER AS tick_spacing,
  t.value :token0 :: STRING AS token0,
  t.value :token0_decimals :: INTEGER AS token0_decimals,
  t.value :token0_name :: STRING AS token0_name,
  t.value :token0_symbol :: STRING AS token0_symbol,
  t.value :token1 :: STRING AS token1,
  t.value :token1_decimals :: INTEGER AS token1_decimals,
  t.value :token1_name :: STRING AS token1_name,
  t.value :token1_symbol :: STRING AS token1_symbol,
  t.value :tx_id :: STRING AS tx_id
FROM
  base_tables,
  LATERAL FLATTEN(
    input => record_content :results
  ) t
