{% macro uniswapv3_lp_actions(src_liquidity_actions_table, src_pools_table, src_prices_table) -%}

WITH prices AS ({{ safe_ethereum_prices(src_prices_table, '30 days', '9 months') }})

SELECT
  a.blockchain,
  a.block_id,
  a.block_timestamp,
  a.tx_id,
  a.action,
  CASE WHEN a.amount0 IS NOT NULL AND p.token0_decimals IS NOT NULL
    THEN a.amount0 / pow(10, p.token0_decimals)
    ELSE NULL
  END AS amount0_adjusted,
  CASE WHEN a.amount1 IS NOT NULL AND p.token1_decimals IS NOT NULL
    THEN a.amount1 / pow(10, p.token1_decimals)
    ELSE NULL
  END AS amount1_adjusted,
  {{ multiply("amount0_adjusted", "prices_0.price")}} as amount0_usd,
  {{ multiply("amount1_adjusted", "prices_1.price")}} as amount1_usd,
  p.token0 AS token0_address, 
  p.token1 AS token1_address, 
  p.token0_symbol, 
  p.token1_symbol,
  prices_0.price AS token0_price,
  prices_1.price AS token1_price,
  a.liquidity_adjusted,
  a.liquidity_provider,
  a.nf_position_manager_address,
  a.nf_token_id,
  a.pool_address,
  p.pool_name,
  a.tick_lower,
  a.tick_upper,
  CASE WHEN a.tick_lower IS NOT NULL AND p.token1_decimals IS NOT NULL AND p.token0_decimals IS NOT NULL
	  THEN pow(1.0001, a.tick_lower) / pow(10, p.token1_decimals - p.token0_decimals)
    ELSE NULL
  END AS price_lower_1_0,
  CASE WHEN a.tick_upper IS NOT NULL AND p.token1_decimals IS NOT NULL AND p.token0_decimals IS NOT NULL
    THEN pow(1.0001, a.tick_upper) / pow(10, p.token1_decimals - p.token0_decimals)
    ELSE NULL
  END AS price_upper_1_0,
  1 / price_upper_1_0 as price_lower_0_1,
  1 / price_lower_1_0 as price_upper_0_1,
  {{ multiply("price_lower_1_0", "prices_1.price")}} as price_lower_1_0_usd,
  {{ multiply("price_upper_1_0", "prices_1.price")}} as price_upper_1_0_usd,
  {{ multiply("price_lower_0_1", "prices_0.price")}} as price_lower_0_1_usd,
  {{ multiply("price_upper_0_1", "prices_0.price")}} as price_upper_0_1_usd
FROM {{ src_liquidity_actions_table }} a 

JOIN {{ src_pools_table }} p 
  ON a.pool_address = p.pool_address

LEFT OUTER JOIN prices prices_0
ON prices_0.hour = date_trunc('day', a.block_timestamp) AND p.token0 = prices_0.token_address

LEFT OUTER JOIN prices prices_1
ON prices_1.hour = date_trunc('day', a.block_timestamp) AND p.token1 = prices_1.token_address

WHERE
{% if is_incremental() %}
  a.block_timestamp >= getdate() - interval '7 days'
{% else %}
  a.block_timestamp >= getdate() - interval '9 months'
{% endif %}


{%- endmacro %}
