{{ config(
  materialized = 'incremental',
  unique_key = 'pool_address || block_id',
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp', 'block_id'],
  tags = ['snowflake', 'uniswapv3_silver', 'silver_uniswapv3__pool_stats']
) }}

WITH silver AS (

  SELECT
    system_created_at,
    block_id,
    block_timestamp,
    blockchain,
    fee_growth_global0_x128,
    fee_growth_global1_x128,
    observation_cardinality,
    observation_cardinality_next,
    observation_index,
    pool_address,
    price,
    price_0_1,
    price_1_0,
    protocol_fees_token0,
    protocol_fees_token1,
    sqrt_price_x96,
    tick,
    token0_balance,
    token1_balance,
    unlocked,
    virtual_liquidity,
    virtual_liquidity_adjusted,
    virtual_reserves_token0,
    virtual_reserves_token1
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
UNION ALL
SELECT
  '2000-01-01' :: TIMESTAMP AS system_created_at,
  block_id,
  block_timestamp,
  blockchain,
  fee_growth_global0_x128,
  fee_growth_global1_x128,
  observation_cardinality,
  observation_cardinality_next,
  observation_index,
  pool_address,
  price,
  price_0_1,
  price_1_0,
  protocol_fees_token0,
  protocol_fees_token1,
  sqrt_price_x96,
  tick,
  token0_balance,
  token1_balance,
  unlocked,
  virtual_liquidity,
  virtual_liquidity_adjusted,
  virtual_reserves_token0,
  virtual_reserves_token1
FROM
  {{ source(
    'uniswapv3_eth',
    'uniswapv3_pool_stats'
  ) }}

{% if is_incremental() %}
AND system_created_at :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(system_created_at :: DATE))
  FROM
    {{ this }} AS pool_stats
)
{% endif %}
)
SELECT
  system_created_at,
  block_id,
  block_timestamp,
  blockchain,
  fee_growth_global0_x128,
  fee_growth_global1_x128,
  observation_cardinality,
  observation_cardinality_next,
  observation_index,
  pool_address,
  price,
  price_0_1,
  price_1_0,
  protocol_fees_token0,
  protocol_fees_token1,
  sqrt_price_x96,
  tick,
  token0_balance,
  token1_balance,
  unlocked,
  virtual_liquidity,
  virtual_liquidity_adjusted,
  virtual_reserves_token0,
  virtual_reserves_token1
FROM
  silver qualify(ROW_NUMBER() over(PARTITION BY pool_address, block_id
ORDER BY
  system_created_at DESC)) = 1
