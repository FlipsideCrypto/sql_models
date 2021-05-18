{% macro uniswapv3_pool_stats(src_pool_stats_table, src_pools_table, src_prices_table) -%}


WITH prices AS ({{ safe_ethereum_prices(src_prices_table, '30 days', '9 months') }})

SELECT
  stats.blockchain,
  stats.block_id,
  stats.block_timestamp,
  stats.fee_growth_global0_x128,
  stats.fee_growth_global1_x128,
  stats.pool_address,
  p.pool_name,
  -- stats.price_0_1,
  -- stats.price AS price_1_0, 
  {{ uni_math_tick_to_price_0_1("p.token0_decimals", "p.token1_decimals", "stats.tick")}} as price_0_1,
  {{ uni_math_tick_to_price_1_0("p.token0_decimals", "p.token1_decimals", "stats.tick")}} as price_1_0,
  CASE WHEN stats.protocol_fees_token0 IS NOT NULL AND p.token0_decimals IS NOT NULL
    THEN stats.protocol_fees_token0 / pow(10, p.token0_decimals)
    ELSE NULL
  END AS protocol_fees_token0_adjusted,
  CASE WHEN stats.protocol_fees_token1 IS NOT NULL AND p.token1_decimals IS NOT NULL
    THEN stats.protocol_fees_token1 / pow(10, p.token1_decimals) 
    ELSE NULL
  END AS protocol_fees_token1_adjusted,
  p.token0 AS token0_address,
  p.token1 AS token1_address,
  p.token0_symbol, 
  p.token1_symbol,
  stats.tick,
  stats.unlocked,
  stats.virtual_liquidity_adjusted,
  CASE WHEN stats.virtual_reserves_token0 IS NOT NULL AND p.token0_decimals IS NOT NULL
    THEN stats.virtual_reserves_token0 / pow(10, p.token0_decimals)
    ELSE NULL
  END AS virtual_reserves_token0_adjusted,
  CASE WHEN stats.virtual_reserves_token1 IS NOT NULL AND p.token1_decimals IS NOT NULL
    THEN stats.virtual_reserves_token1 / pow(10, p.token1_decimals)
    ELSE NULL
  END AS virtual_reserves_token1_adjusted,
  {{ multiply("virtual_reserves_token0_adjusted", "prices_0.price")}} as virtual_reserves_token0_usd,
  {{ multiply("virtual_reserves_token1_adjusted", "prices_1.price")}} as virtual_reserves_token1_usd,
  {{ decimal_adjust("stats.token0_balance", "p.token0_decimals") }} AS token0_balance_adjusted,
  {{ decimal_adjust("stats.token1_balance", "p.token1_decimals") }} AS token1_balance_adjusted,
  {{ multiply("token0_balance_adjusted", "prices_0.price")}} as token0_balance_usd,
  {{ multiply("token1_balance_adjusted", "prices_1.price")}} as token1_balance_usd,
  stats.token0_balance,
  stats.token1_balance
FROM {{ src_pool_stats_table }} stats 

JOIN {{ src_pools_table }} p 
  ON stats.pool_address = p.pool_address

LEFT OUTER JOIN prices prices_1
  ON prices_1.hour = date_trunc('day', stats.block_timestamp) AND p.token1 = prices_1.token_address

LEFT OUTER JOIN prices prices_0
  ON prices_0.hour = date_trunc('day', stats.block_timestamp) AND p.token0 = prices_0.token_address

WHERE
{% if is_incremental() %}
  stats.block_timestamp >= getdate() - interval '7 days'
{% else %}
  stats.block_timestamp >= getdate() - interval '9 months'
{% endif %}

-- usd price will be added in later version (V2)

{%- endmacro %}
