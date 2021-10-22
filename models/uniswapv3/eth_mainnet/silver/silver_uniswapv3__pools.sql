{{ config(
  materialized = 'incremental',
  unique_key = 'pool_address',
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp', 'block_id'],
  tags = ['snowflake', 'uniswapv3_silver', 'silver_uniswapv3__pools']
) }}

WITH silver AS (

  SELECT
    system_created_at,
    block_id,
    block_timestamp,
    blockchain,
    factory_address,
    fee,
    init_price,
    init_sqrt_price_x96,
    init_tick,
    pool_address,
    pool_name,
    tick_spacing,
    token0,
    token0_decimals,
    token0_name,
    token0_symbol,
    token1,
    token1_decimals,
    token1_name,
    token1_symbol,
    tx_id
  FROM
    {{ ref('uniswapv3_dbt__pools') }}
  WHERE
    1 = 1

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(block_timestamp :: DATE))
  FROM
    {{ this }} AS pools
)
{% endif %}
UNION ALL
SELECT
  '2000-01-01' :: TIMESTAMP AS system_created_at,
  block_id,
  block_timestamp,
  blockchain,
  factory_address,
  fee,
  init_price,
  init_sqrt_price_x96,
  init_tick,
  pool_address,
  pool_name,
  tick_spacing,
  token0,
  token0_decimals,
  token0_name,
  token0_symbol,
  token1,
  token1_decimals,
  token1_name,
  token1_symbol,
  tx_id
FROM
  {{ source(
    'uniswapv3_eth',
    'uniswapv3_pools'
  ) }}
WHERE
  1 = 1

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(block_timestamp :: DATE))
  FROM
    {{ this }} AS pools
)
{% endif %}
)
SELECT
  system_created_at,
  block_id,
  block_timestamp,
  blockchain,
  factory_address,
  fee,
  init_price,
  init_sqrt_price_x96,
  init_tick,
  pool_address,
  pool_name,
  tick_spacing,
  token0,
  token0_decimals,
  token0_name,
  token0_symbol,
  token1,
  token1_decimals,
  token1_name,
  token1_symbol,
  tx_id
FROM
  silver qualify(ROW_NUMBER() over(PARTITION BY pool_address
ORDER BY
  system_created_at DESC)) = 1
