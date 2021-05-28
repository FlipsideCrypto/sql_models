{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key= 'tx_id', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'ethereum', 'dex']
  )
}}

 SELECT
 
    block_timestamp, 
    p.contract_address AS pool_address,
    tx_id, 
    REGEXP_REPLACE(event_inputs:caller,'\"','') AS from_address,
    tx_from_address AS to_address, -- weirdly this seems to be right, see this tx run through 1inch by a user on balancer 0x968e86d6a587150159c8a0f6c96e4f5f277b303cf150bf4bc6d793f64d8d51dd
    -- token in 
    event_inputs:tokenAmountIn / POWER(10, price0.decimals) 
    AS amount_in,
    event_inputs:tokenAmountIn * price0.price / POWER(10, price0.decimals) AS amount_in_usd,
    REGEXP_REPLACE(p.event_inputs:tokenIn,'\"','') AS token_in,
    -- token out
    event_inputs:tokenAmountOut / POWER(10, price1.decimals) 
    AS amount_in,
    event_inputs:tokenAmountOut * price1.price / POWER(10, price1.decimals) AS amount_in_usd,
    REGEXP_REPLACE(p.event_inputs:tokenOut,'\"','') AS token_out,
    
    'balancer-v1' AS platform
  FROM {{ref('ethereum__events_emitted')}} p
  
  LEFT JOIN {{ref('ethereum__token_prices_hourly')}} price0 
  ON p.event_inputs:tokenIn = price0.token_address AND DATE_TRUNC('hour',p.block_timestamp) = price0.hour
  LEFT JOIN {{ref('ethereum__token_prices_hourly')}} price1 
  ON p.event_inputs:tokenOut = price1.token_address AND DATE_TRUNC('hour',p.block_timestamp) = price1.hour
  
  WHERE event_name = 'LOG_SWAP'
  {% if is_incremental() %}
    AND block_timestamp >= getdate() - interval '2 days'
  {% else %}
    AND block_timestamp >= getdate() - interval '9 months'
  {% endif %}
  AND event_inputs:caller IS NOT NULL