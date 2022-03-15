{% macro uniswapv3_pools(src_pools_table, src_prices_table) -%}

WITH prices AS ({{ safe_ethereum_prices(src_prices_table, '30 days', '9 months') }})
SELECT
  blockchain,
  block_id,
  block_timestamp,
  tx_id,
  factory_address,
  CASE WHEN fee IS NOT NULL
	  THEN fee / 10000
    ELSE NULL
  END AS fee_percent,
  init_price AS init_price_1_0,
  {{ multiply("init_price", "prices_1.price")}} as init_price_1_0_usd,
  init_tick,
  pool_address,
  pool_name,
  tick_spacing,
  token0 AS token0_address,
  token1 AS token1_address,
  token0_symbol, 
  token1_symbol,
  token0_name,
  token1_name,
  token0_decimals,
  token1_decimals
FROM {{ src_pools_table }} p
LEFT OUTER JOIN prices prices_1
    ON prices_1.hour = date_trunc('day', p.block_timestamp) AND p.token1 = prices_1.token_address

WHERE
{% if is_incremental() %}
  block_timestamp >= getdate() - interval '7 days'
{% else %}
  TRUE
{% endif %}


{%- endmacro %}
