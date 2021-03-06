{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', pool_address, block_id)",
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
AND block_timestamp :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(block_timestamp:: DATE))
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
WHERE
  1 = 1

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(block_timestamp :: DATE))
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
  COALESCE(fee_growth_global0_x128, 0) as fee_growth_global0_x128,
  COALESCE(fee_growth_global1_x128, 0) as fee_growth_global1_x128,
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
  COALESCE(token0_balance, 0) as token0_balance,
  COALESCE(token1_balance, 0) as token1_balance,
  unlocked,
  COALESCE(virtual_liquidity, 0) as virtual_liquidity, 
  virtual_liquidity_adjusted,
  virtual_reserves_token0,
  virtual_reserves_token1
FROM
  silver qualify(ROW_NUMBER() over(PARTITION BY pool_address, block_id
ORDER BY
  system_created_at DESC)) = 1
