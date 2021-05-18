{% macro uniswapv3_positions(src_positions_table, src_pools_table, src_prices_table) -%}

WITH prices AS ({{ safe_ethereum_prices(src_prices_table, '30 days', '9 months') }})

SELECT  
	pos.blockchain,
	pos.block_id,
	pos.block_timestamp,
	pos.tx_id,
  CASE WHEN pos.fee IS NOT NULL
	  THEN pos.fee / 10000
    ELSE NULL
  END AS fee_percent,
	pos.fee_growth_inside0_last_x128, 
	pos.fee_growth_inside1_last_x128, 
	pos.is_active,
	pos.liquidity_adjusted,
	pos.liquidity_provider,
	pos.nf_position_manager_address,
	pos.nf_token_id,
	pos.pool_address,
	p.pool_name,
  pos.tick_upper,
	pos.tick_lower,
  {{ uni_math_tick_to_price_1_0("p.token0_decimals", "p.token1_decimals", "pos.tick_upper")}} as price_upper_1_0,
  {{ uni_math_tick_to_price_1_0("p.token0_decimals", "p.token1_decimals", "pos.tick_lower")}} as price_lower_1_0,
  1 / price_lower_1_0 as price_upper_0_1,
  1 / price_upper_1_0 as price_lower_0_1,
  {{ multiply("price_upper_1_0", "prices_1.price")}} as price_upper_1_0_usd,
  {{ multiply("price_lower_1_0", "prices_1.price")}} as price_lower_1_0_usd,
  {{ multiply("price_upper_0_1", "prices_0.price")}} as price_upper_0_1_usd,
  {{ multiply("price_lower_0_1", "prices_0.price")}} as price_lower_0_1_usd,
  CASE WHEN pos.tokens_owed0 IS NOT NULL AND p.token0_decimals IS NOT NULL
    THEN pos.tokens_owed0 / pow(10, p.token0_decimals)
    ELSE NULL
  END AS tokens_owed0_adjusted,
  CASE WHEN pos.tokens_owed1 IS NOT NULL AND p.token1_decimals IS NOT NULL
    THEN pos.tokens_owed1 / pow(10, p.token1_decimals)
    ELSE NULL
  END AS tokens_owed1_adjusted,
  {{ multiply("tokens_owed0_adjusted", "prices_0.price")}} as tokens_owed0_usd,
  {{ multiply("tokens_owed1_adjusted", "prices_1.price")}} as tokens_owed1_usd,
	p.token0 AS token0_address,
	p.token1 AS token1_address,
	p.token0_symbol, 
	p.token1_symbol
FROM {{ src_positions_table }} pos

JOIN {{ src_pools_table }} p 
	ON pos.pool_address = p.pool_address

LEFT OUTER JOIN prices prices_0
        ON prices_0.hour = date_trunc('day', pos.block_timestamp) AND p.token0 = prices_0.token_address
LEFT OUTER JOIN prices prices_1
    ON prices_1.hour = date_trunc('day', pos.block_timestamp) AND p.token1 = prices_1.token_address

WHERE
{% if is_incremental() %}
  pos.block_timestamp >= getdate() - interval '7 days'
{% else %}
  pos.block_timestamp >= getdate() - interval '9 months'
{% endif %}


{%- endmacro %}
