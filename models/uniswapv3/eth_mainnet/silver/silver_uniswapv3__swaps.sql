{{ config(
  materialized = 'incremental',
  unique_key = 'tx_id || log_index',
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp', 'block_id'],
  tags = ['snowflake', 'uniswapv3_silver', 'silver_uniswapv3__swaps']
) }}

WITH silver AS (

  SELECT
    system_created_at,
    amount0,
    amount1,
    block_id,
    block_timestamp,
    blockchain,
    liquidity,
    liqudity_adjusted,
    log_index,
    pool_address,
    price_0_1,
    price_1_0,
    recipient,
    sender,
    sqrt_price_x96,
    tick,
    tx_id
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
UNION ALL
SELECT
  '2000-01-01' :: TIMESTAMP AS system_created_at,
  amount0,
  amount1,
  block_id,
  block_timestamp,
  blockchain,
  liquidity,
  liqudity_adjusted,
  log_index,
  pool_address,
  price_0_1,
  price_1_0,
  recipient,
  sender,
  sqrt_price_x96,
  tick,
  tx_id
FROM
  {{ source(
    'uniswapv3_eth',
    'uniswapv3_swaps'
  ) }}

{% if is_incremental() %}
AND system_created_at :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(system_created_at :: DATE))
  FROM
    {{ this }} AS swaps
)
{% endif %}
)
SELECT
  system_created_at,
  amount0,
  amount1,
  block_id,
  block_timestamp,
  blockchain,
  liquidity,
  liqudity_adjusted,
  log_index,
  pool_address,
  price_0_1,
  price_1_0,
  recipient,
  sender,
  sqrt_price_x96,
  tick,
  tx_id
FROM
  silver qualify(ROW_NUMBER() over(PARTITION BY tx_id, log_index
ORDER BY
  system_created_at DESC)) = 1
