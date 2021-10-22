{{ config(
  materialized = 'incremental',
  unique_key = 'tx_id || nf_token_id',
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp', 'block_id'],
  tags = ['snowflake', 'uniswapv3_silver', 'silver_uniswapv3__liquidity_actions']
) }}

WITH silver AS (

  SELECT
    system_created_at,
    action,
    amount0,
    amount1,
    block_id,
    block_timestamp,
    blockchain,
    liquidity,
    liquidity_adjusted,
    liquidity_provider,
    nf_position_manager_address,
    nf_token_id,
    pool_address,
    price,
    sqrt_price_x96,
    tick_lower,
    tick_upper,
    tx_id,
    virtual_reserves_token0,
    virtual_reserves_token1
  FROM
    {{ ref('uniswapv3_dbt__liquidity_actions') }}
  WHERE
    1 = 1

{% if is_incremental() %}
AND system_created_at :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(system_created_at :: DATE))
  FROM
    {{ this }} AS liquidity_actions
)
{% endif %}
UNION ALL
SELECT
  '2000-01-01' :: TIMESTAMP AS system_created_at,
  action,
  amount0,
  amount1,
  block_id,
  block_timestamp,
  blockchain,
  liquidity,
  liquidity_adjusted,
  liquidity_provider,
  nf_position_manager_address,
  nf_token_id,
  pool_address,
  price,
  sqrt_price_x96,
  tick_lower,
  tick_upper,
  tx_id,
  virtual_reserves_token0,
  virtual_reserves_token1
FROM
  {{ source(
    'uniswapv3_eth',
    'uniswapv3_liquidity_actions'
  ) }}
WHERE
  1 = 1

{% if is_incremental() %}
AND system_created_at :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(system_created_at :: DATE))
  FROM
    {{ this }} AS liquidity_actions
)
{% endif %}
)
SELECT
  system_created_at,
  action,
  amount0,
  amount1,
  block_id,
  block_timestamp,
  blockchain,
  liquidity,
  liquidity_adjusted,
  liquidity_provider,
  nf_position_manager_address,
  nf_token_id,
  pool_address,
  price,
  sqrt_price_x96,
  tick_lower,
  tick_upper,
  tx_id,
  virtual_reserves_token0,
  virtual_reserves_token1
FROM
  silver qualify(ROW_NUMBER() over(PARTITION BY tx_id, nf_token_id
ORDER BY
  system_created_at DESC)) = 1
