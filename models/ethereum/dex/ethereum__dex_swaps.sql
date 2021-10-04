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
WITH decimals_raw as (

  SELECT address AS token_address,
  meta:decimals AS decimals,
  2 as weight
  FROM {{source('ethereum', 'ethereum_contracts')}}
  WHERE meta:decimals IS NOT NULL

  UNION

  SELECT DISTINCT token_address,
  decimals,
  1 AS weight
  FROM {{ref('ethereum__token_prices_hourly')}} 
  WHERE 
  {% if is_incremental() %}
    hour >= getdate() - interval '31 days' AND
  {% else %}
    -- hour >= getdate() - interval '12 months'
  {% endif %}
   decimals IS NOT NULL

), decimals AS (
  SELECT token_address,decimals,weight
  FROM decimals_raw
  QUALIFY (row_number() OVER (partition by token_address order by weight desc)) = 1
),
 usd_swaps AS (
  SELECT DISTINCT
    block_timestamp, 
    p.pool_address,
    p.pool_name,
    p.token0 AS token_address,
    tx_id, 
    event_inputs:amount0In / POWER(10, d0.decimals) AS amount_in,
    event_inputs:amount0Out / POWER(10, d0.decimals) AS amount_out, 
    REGEXP_REPLACE(event_inputs:sender,'\"','') AS from_address,
    REGEXP_REPLACE(event_inputs:to,'\"','') AS to_address,
    CASE WHEN event_inputs:amount0In > 0 THEN event_inputs:amount0In * price0.price / POWER(10, d0.decimals) 
         ELSE event_inputs:amount0Out * price0.price / POWER(10, d0.decimals) END
    AS amount_usd,
    --CASE WHEN event_inputs:amount1In > 0 THEN event_inputs:amount1In * price1.price / POWER(10, d1.decimals) 
    --     ELSE event_inputs:amount1Out * price1.price / POWER(10, d1.decimals) END
    --AS other_amount_usd,
    CASE WHEN p.factory_address = '0xc0aee478e3658e2610c5f7a4a2e1777ce9e4f2ac' THEN 'sushiswap' ELSE 'uniswap-v2' END AS platform,
    event_index,
    CASE WHEN event_inputs:amount0In > 0 THEN 'IN' 
    ELSE 'OUT' END AS direction
  FROM {{ref('ethereum__events_emitted')}} s0

  LEFT JOIN {{ref('ethereum__dex_liquidity_pools')}} p 
    ON s0.contract_address = p.pool_address

  LEFT JOIN {{ref('ethereum__token_prices_hourly')}} price0 
    ON p.token0 = price0.token_address AND DATE_TRUNC('hour',s0.block_timestamp) = price0.hour

  LEFT JOIN decimals d0
    ON p.token0 = d0.token_address

  --LEFT JOIN {{ref('ethereum__token_prices_hourly')}} price1
  --  ON p.token1 = price1.token_address AND DATE_TRUNC('hour',s0.block_timestamp) = price0.hour

  --LEFT JOIN decimals d1
  --  ON p.token1 = d1.token_address

  WHERE event_name = 'Swap' AND platform <> 'uniswap-v3' 

  {% if is_incremental() %}
    AND block_timestamp >= getdate() - interval '2 days'
  {% else %}
    AND block_timestamp >= getdate() - interval '9 months'
  {% endif %}

  UNION

  SELECT DISTINCT
    block_timestamp, 
    p.pool_address,
    p.pool_name,
    p.token1 AS token_address,
    tx_id, 
    event_inputs:amount1In / POWER(10, d1.decimals) AS amount_in,
    event_inputs:amount1Out / POWER(10, d1.decimals) AS amount_out, 
    REGEXP_REPLACE(event_inputs:sender,'\"','') AS from_address,
    REGEXP_REPLACE(event_inputs:to,'\"','') AS to_address,
    CASE WHEN event_inputs:amount1In > 0 THEN event_inputs:amount1In * price1.price / POWER(10, d1.decimals) 
         ELSE event_inputs:amount1Out * price1.price / POWER(10, d1.decimals) END
    AS amount_usd,
    -- CASE WHEN event_inputs:amount1In > 0 THEN event_inputs:amount1In * price1.price / POWER(10, d1.decimals) 
    --     ELSE event_inputs:amount1Out * price1.price / POWER(10, d1.decimals) END
    --AS other_amount_usd,
    CASE WHEN p.factory_address = '0xc0aee478e3658e2610c5f7a4a2e1777ce9e4f2ac' THEN 'sushiswap' ELSE 'uniswap-v2' END AS platform,
    event_index,
    CASE WHEN event_inputs:amount1In > 0 THEN 'IN' 
    ELSE 'OUT' END AS direction

  FROM  {{ref('ethereum__events_emitted')}} s0

  LEFT JOIN {{ref('ethereum__dex_liquidity_pools')}} p 
    ON s0.contract_address = p.pool_address

  --LEFT JOIN {{ref('ethereum__token_prices_hourly')}} price0 
  --  ON p.token0 = price0.token_address AND DATE_TRUNC('hour',s0.block_timestamp) = price0.hour

  --LEFT JOIN decimals d0
  --  ON p.token0 = d0.token_address

  LEFT JOIN {{ref('ethereum__token_prices_hourly')}} price1
    ON p.token1 = price1.token_address AND DATE_TRUNC('hour',s0.block_timestamp) = price1.hour

  LEFT JOIN decimals d1
    ON p.token1 = d1.token_address
  

  WHERE 
      event_name = 'Swap' AND platform <> 'uniswap-v3' 
  {% if is_incremental() %}
    AND block_timestamp >= getdate() - interval '2 days'
  {% else %}
    AND block_timestamp >= getdate() - interval '9 months'
  {% endif %}

), v3_swaps AS (

  SELECT DISTINCT
    s.block_timestamp,s.pool_address, -- token 0 v3 swaps
    dl.pool_name, -- get from lp table
    dl.token0 AS token_address,
    s.tx_id,
    CASE WHEN amount0_adjusted > 0 THEN amount0_adjusted ELSE NULL END  AS amount_in,
    CASE WHEN amount0_adjusted <= 0 THEN -1*amount0_adjusted ELSE NULL END  AS amount_out,
    sender AS from_address,
    recipient AS to_address,
    COALESCE(amount_in,amount_out)*p.price AS amount_usd,
    'uniswap-v3' AS platform,
    ind.event_index,
    CASE WHEN amount0_adjusted > 0 THEN 'IN' 
    ELSE 'OUT' END AS direction
  FROM 
  -- {{source('uniswapv3_eth','uniswapv3_swaps')}} 
  {{ref('uniswapv3__swaps')}} s
  LEFT JOIN {{ref('ethereum__dex_liquidity_pools')}} dl 
    ON s.pool_address = dl.pool_address
  LEFT JOIN {{ref('ethereum__token_prices_hourly')}} p 
    ON dl.token0 = p.token_address  AND p.hour = date_trunc('hour',block_timestamp)
  LEFT JOIN {{ref('ethereum__events_emitted')}} ind
    ON s.pool_address = ind.contract_address AND s.tx_id = ind.tx_id
    WHERE (amount1_adjusted > 0 OR amount0_adjusted > 0) AND platform = 'uniswap-v3' 

  UNION

  SELECT DISTINCT
    s.block_timestamp,s.pool_address, -- token1 v3 swaps
    dl.pool_name, -- get from lp table
    dl.token1 AS token_address,
    s.tx_id,
    CASE WHEN amount1_adjusted > 0 THEN amount1_adjusted ELSE NULL END  AS amount_in,
    CASE WHEN amount1_adjusted <= 0 THEN -1*amount1_adjusted ELSE NULL END  AS amount_out,
    sender AS from_address,
    recipient AS to_address,
    COALESCE(amount_in,amount_out)*p.price AS amount_usd ,
    'uniswap-v3' AS platform,
    ind.event_index,
    CASE WHEN amount1_adjusted > 0 THEN 'IN' 
    ELSE 'OUT' END AS direction
  FROM 
  -- {{source('uniswapv3_eth','uniswapv3_swaps')}} s
  {{ref('uniswapv3__swaps')}} s
  LEFT JOIN {{ref('ethereum__dex_liquidity_pools')}} dl 
    ON s.pool_address = dl.pool_address
  LEFT JOIN {{ref('ethereum__token_prices_hourly')}} p 
    ON dl.token1 = p.token_address  AND p.hour = date_trunc('hour',block_timestamp)
  LEFT JOIN {{ref('ethereum__events_emitted')}} ind
    ON s.pool_address = ind.contract_address AND s.tx_id = ind.tx_id
    WHERE (amount1_adjusted > 0 OR amount0_adjusted > 0) AND platform = 'uniswap-v3' 
)

SELECT 
  block_timestamp,pool_address,
  pool_name,
  token_address,
  tx_id,
  amount_in,amount_out,
  from_address,
  to_address,
  -- CASE WHEN ((amount_usd - other_amount_usd) / amount_usd) > .15 THEN other_amount_usd
  -- ELSE amount_usd END AS amount_usd,
  amount_usd,
  platform,
  event_index,
  direction
FROM usd_swaps
WHERE pool_address NOT IN ('0xdc6a5faf34affccc6a00d580ecb3308fc1848f22') -- stop-gap for big price swings, the actual solution adds an enormous amount of runtime

UNION

SELECT 
  block_timestamp,
  pool_address,
  pool_name,
  token_address,
  tx_id,
  amount_in,
  amount_out,
  from_address,
  to_address,
  amount_usd,
  platform,
  event_index,
  direction
FROM v3_swaps
