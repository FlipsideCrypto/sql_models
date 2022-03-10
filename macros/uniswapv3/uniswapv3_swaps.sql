{% macro uniswapv3_swaps(src_swaps_table, src_pools_table, src_prices_table) -%}

WITH prices AS ({{ safe_ethereum_prices(src_prices_table, '30 days', '9 months') }})

SELECT 
  swaps.blockchain,
  swaps.block_id,
  swaps.block_timestamp,
  swaps.tx_id,
  swaps.pool_address,
  p.pool_name,
  swaps.price AS price_1_0,
  swaps.price_0_1,
  swaps.recipient,
  swaps.sender,
  swaps.tick,
  swaps.liquidity,
  swaps.liquidity_adjusted,
  log_index,
  {{ decimal_adjust("swaps.amount0", "p.token0_decimals") }} AS amount0_adjusted,
  {{ decimal_adjust("swaps.amount1", "p.token1_decimals") }} AS amount1_adjusted,
  p.token0 AS token0_address,
  p.token1 AS token1_address,
  p.token0_symbol, 
  p.token1_symbol,
  CASE 
    WHEN prices_0.price IS NULL AND prices_1.price IS NOT NULL
      THEN prices_1.price / swaps.price_0_1
      ELSE prices_0.price
  END AS token0_price,
  CASE 
    WHEN prices_1.price IS NULL AND prices_0.price IS NOT NULL
      THEN prices_0.price / swaps.price
      ELSE prices_1.price
  END AS token1_price,
  {{ multiply("amount0_adjusted", "token0_price")}} as amount0_usd,
  {{ multiply("amount1_adjusted", "token1_price")}} as amount1_usd

FROM {{ src_swaps_table }} swaps

JOIN {{ src_pools_table }} p 
  ON swaps.pool_address = p.pool_address

LEFT OUTER JOIN prices prices_0
ON prices_0.hour = date_trunc('day', swaps.block_timestamp) AND p.token0 = prices_0.token_address

LEFT OUTER JOIN prices prices_1
ON prices_1.hour = date_trunc('day', swaps.block_timestamp) AND p.token1 = prices_1.token_address

WHERE
{% if is_incremental() %}
  swaps.block_timestamp >= getdate() - interval '7 days'
{% else %}
  swaps.block_timestamp >= getdate() - interval '9 months'
{% endif %}

{%- endmacro %}
