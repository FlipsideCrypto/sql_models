{% macro uniswapv3_lp_actions(
    src_liquidity_actions_table,
    src_pools_table,
    src_prices_table
  ) -%}
  WITH prices AS (
    {{ safe_ethereum_prices(
      src_prices_table,
      '30 days',
      '9 months'
    ) }}
  )
SELECT
  A.blockchain,
  A.block_id,
  A.block_timestamp,
  A.tx_id,
  A.action,
  CASE
    WHEN A.amount0 IS NOT NULL
    AND p.token0_decimals IS NOT NULL THEN A.amount0 / pow(
      10,
      p.token0_decimals
    )
    ELSE NULL
  END AS amount0_adjusted,
  CASE
    WHEN A.amount1 IS NOT NULL
    AND p.token1_decimals IS NOT NULL THEN A.amount1 / pow(
      10,
      p.token1_decimals
    )
    ELSE NULL
  END AS amount1_adjusted,
  {{ multiply(
    "amount0_adjusted",
    "prices_0.price"
  ) }} AS amount0_usd,
  {{ multiply(
    "amount1_adjusted",
    "prices_1.price"
  ) }} AS amount1_usd,
  p.token0 AS token0_address,
  p.token1 AS token1_address,
  p.token0_symbol,
  p.token1_symbol,
  prices_0.price AS token0_price,
  prices_1.price AS token1_price,
  A.liquidity AS liquidity,
  A.liquidity_adjusted,
  A.liquidity_provider,
  A.nf_position_manager_address,
  A.nf_token_id,
  A.pool_address,
  p.pool_name,
  A.tick_lower,
  A.tick_upper,
  CASE
    WHEN A.tick_lower IS NOT NULL
    AND p.token1_decimals IS NOT NULL
    AND p.token0_decimals IS NOT NULL THEN pow(
      1.0001,
      A.tick_lower
    ) / pow(
      10,
      p.token1_decimals - p.token0_decimals
    )
    ELSE NULL
  END AS price_lower_1_0,
  CASE
    WHEN A.tick_upper IS NOT NULL
    AND p.token1_decimals IS NOT NULL
    AND p.token0_decimals IS NOT NULL THEN pow(
      1.0001,
      A.tick_upper
    ) / pow(
      10,
      p.token1_decimals - p.token0_decimals
    )
    ELSE NULL
  END AS price_upper_1_0,
  1 / price_upper_1_0 AS price_lower_0_1,
  1 / price_lower_1_0 AS price_upper_0_1,
  {{ multiply(
    "price_lower_1_0",
    "prices_1.price"
  ) }} AS price_lower_1_0_usd,
  {{ multiply(
    "price_upper_1_0",
    "prices_1.price"
  ) }} AS price_upper_1_0_usd,
  {{ multiply(
    "price_lower_0_1",
    "prices_0.price"
  ) }} AS price_lower_0_1_usd,
  {{ multiply(
    "price_upper_0_1",
    "prices_0.price"
  ) }} AS price_upper_0_1_usd
FROM
  {{ src_liquidity_actions_table }} A
  JOIN {{ src_pools_table }}
  p
  ON A.pool_address = p.pool_address
  LEFT OUTER JOIN prices prices_0
  ON prices_0.hour = DATE_TRUNC(
    'day',
    A.block_timestamp
  )
  AND p.token0 = prices_0.token_address
  LEFT OUTER JOIN prices prices_1
  ON prices_1.hour = DATE_TRUNC(
    'day',
    A.block_timestamp
  )
  AND p.token1 = prices_1.token_address
WHERE

{% if is_incremental() %}
A.block_timestamp >= getdate() - INTERVAL '7 days'
{% else %}
  A.block_timestamp >= getdate() - INTERVAL '9 months'
{% endif %}
{%- endmacro %}
