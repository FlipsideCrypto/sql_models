{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key= 'tx_id', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'ethereum', 'dex']
  )
}}




-- this will depend on having the liquidity pool table set
-- makes a table of swaps in a long format, i.e. swap_date, pool, token_address, swapped amount
-- this is currently built for uniswap/sushiswap style swaps but that format would also work for pools that allow 2+ tokens (i.e. Curve's 3Pool, Balancer, etc.)
WITH usd_swaps AS (
  SELECT
    block_timestamp, 
    p.pool_address,
    p.pool_name,
    p.token0 AS token_address,
    tx_id, 
    event_inputs:amount0In / POWER(10, price0.decimals) AS amount_in,
    event_inputs:amount0Out / POWER(10, price0.decimals) AS amount_out, 
    REGEXP_REPLACE(event_inputs:sender,'\"','') AS from_address,
    REGEXP_REPLACE(event_inputs:to,'\"','') AS to_address,
    CASE WHEN event_inputs:amount0In > 0 THEN event_inputs:amount0In * price0.price / POWER(10, price0.decimals) 
         ELSE event_inputs:amount0Out * price0.price / POWER(10, price0.decimals) END
    AS amount_usd,
    'uniswap-v2' AS platform
  FROM {{ref('ethereum__events_emitted')}} s0

  LEFT JOIN {{ref('ethereum__dex_liquidity_pools')}} p 
    ON s0.contract_address = p.pool_address

  LEFT JOIN {{ref('ethereum__token_prices_hourly')}} price0 
    ON p.token0 = price0.token_address AND DATE_TRUNC('hour',s0.block_timestamp) = price0.hour

  WHERE event_name = 'Swap'

  {% if is_incremental() %}
    AND block_timestamp >= getdate() - interval '2 days'
  {% else %}
    AND block_timestamp >= getdate() - interval '9 months'
  {% endif %}

  UNION

  SELECT  
    block_timestamp, 
    p.pool_address,
    p.pool_name,
    p.token1 AS token_address,
    tx_id, 
    event_inputs:amount1In / POWER(10, price1.decimals) AS amount_in,
    event_inputs:amount1Out / POWER(10, price1.decimals) AS amount_out, 
    REGEXP_REPLACE(event_inputs:sender,'\"','') AS from_address,
    REGEXP_REPLACE(event_inputs:to,'\"','') AS to_address,
    CASE WHEN event_inputs:amount1In > 0 THEN event_inputs:amount1In * price1.price / POWER(10, price1.decimals) 
         ELSE event_inputs:amount1Out * price1.price / POWER(10, price1.decimals) END
    AS amount_usd,
    'uniswap-v2' AS platform
  FROM  {{ref('ethereum__events_emitted')}} s0

  LEFT JOIN {{ref('ethereum__dex_liquidity_pools')}} p 
    ON s0.contract_address = p.pool_address

  LEFT JOIN {{ref('ethereum__token_prices_hourly')}} price1 
    ON p.token1 = price1.token_address AND DATE_TRUNC('hour',s0.block_timestamp) = price1.hour

  WHERE 
      event_name = 'Swap'
  {% if is_incremental() %}
    AND block_timestamp >= getdate() - interval '2 days'
  {% else %}
    AND block_timestamp >= getdate() - interval '9 months'
  {% endif %}

), v3_swaps AS (

  SELECT 
    block_timestamp,s.pool_address, -- token 0 v3 swaps
    dl.pool_name, -- get from lp table
    dl.token0 AS token_address,
    tx_id,
    CASE WHEN amount0 > 0 THEN amount0 ELSE NULL END  AS amount_in,
    CASE WHEN amount0 <= 0 THEN -1*amount0 ELSE NULL END  AS amount_out,
    sender AS from_address,
    recipient AS to_address,
    COALESCE(amount_in,amount_out)*p.price AS amount_usd,
    'uniswap-v3' AS platform
  FROM {{source('uniswapv3_eth','uniswapv3_swaps')}} s
  LEFT JOIN {{ref('ethereum__dex_liquidity_pools')}} dl 
    ON s.pool_address = dl.pool_address
  LEFT JOIN {{ref('ethereum__token_prices_hourly')}} p 
    ON dl.token0 = p.token_address  AND p.hour = date_trunc('hour',block_timestamp)

  UNION

  SELECT 
    block_timestamp,s.pool_address, -- token1 v3 swaps
    dl.pool_name, -- get from lp table
    dl.token1 AS token_address,
    tx_id,
    CASE WHEN amount1 > 0 THEN amount1 ELSE NULL END  AS amount_in,
    CASE WHEN amount1 <= 0 THEN -1*amount1 ELSE NULL END  AS amount_out,
    sender AS from_address,
    recipient AS to_address,
    COALESCE(amount_in,amount_out)*p.price AS amount_usd ,
    'uniswap-v3' AS platform
  FROM {{source('uniswapv3_eth','uniswapv3_swaps')}} s
  LEFT JOIN {{ref('ethereum__dex_liquidity_pools')}} dl 
    ON s.pool_address = dl.pool_address
  LEFT JOIN {{ref('ethereum__token_prices_hourly')}} p 
    ON dl.token1 = p.token_address  AND p.hour = date_trunc('hour',block_timestamp)

)

SELECT *
FROM usd_swaps

UNION

SELECT * 
FROM v3_swaps
