{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', tx_id, log_index)",
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
    liquidity_adjusted,
    log_index,
    pool_address,
    price,
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
AND block_timestamp :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(block_timestamp :: DATE))
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
  liquidity_adjusted,
  log_index,
  pool_address,
  price,
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
WHERE
  1 = 1

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(block_timestamp :: DATE))
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
  COALESCE(liquidity, 0) as liquidity,
  COALESCE(liquidity_adjusted, 0) as liquidity_adjusted,
  log_index,
  pool_address,
  price,
  price_0_1,
  price_1_0,
  recipient,
  sender,
  sqrt_price_x96,
  tick,
  tx_id
FROM
  silver
WHERE
  price > 0 qualify(ROW_NUMBER() over(PARTITION BY tx_id, log_index
ORDER BY
  system_created_at DESC)) = 1
