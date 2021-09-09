{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key= 'tx_id || event_index', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'ethereum', 'curve']
  )
}}




WITH 
 pool_tokens AS (
    SELECT 
        DISTINCT
            LOWER(inputs:_pool::STRING) AS pool_add, 
                (SPLIT(LOWER(VALUE_STR),'^')) AS coins
    FROM {{source('ethereum', 'ethereum_reads')}}
    WHERE 
      contract_name='Vyper_contract'  
      AND contract_address IN ('0x0959158b6040d32d04c301a72cbfd6b39e21c9ae',
                          LOWER('0xfD6f33A0509ec67dEFc500755322aBd9Df1bD5B8'), 
                              '0x90e00ace148ca3b23ac1bc8c240c2a7dd9c2d7f5','0x7D86446dDb609eD0F5f8684AcF30380a356b2B4c')  
      AND function_name = 'get_underlying_coins'
      AND block_timestamp >= CURRENT_DATE - 60

), pool_tokens_parsed AS (
  SELECT 
      pool_add, 
      CASE WHEN value::STRING = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' ELSE value::STRING END AS coins, 
        (row_number() OVER (partition by pool_add order by pool_add desc) - 1 ) AS index
  FROM pool_tokens, 
  Table(Flatten(pool_tokens.coins))
  WHERE 
    value::STRING <> '0x0000000000000000000000000000000000000000'

), curve_swaps_raw AS (
  SELECT 
    block_id,
    block_timestamp,
    tx_id,
    event_index,
    event_inputs:buyer::STRING AS swapper,
    contract_address AS pool_address,
    event_inputs:bought_id AS index_in, 
    event_inputs:tokens_bought AS amount_in,
    event_inputs:sold_id   AS index_out, 
    event_inputs:tokens_sold AS amount_out,
    event_inputs
  FROM {{ref('ethereum__events_emitted')}} 
  WHERE event_name = 'TokenExchange' 
  AND block_timestamp >= CURRENT_DATE - 360 
  AND event_inputs:buyer IS NOT NULL AND event_inputs:bought_id IS NOT NULL
  
), prices AS (
  SELECT 
    token_address,
    hour,
    AVG(price) AS price,
    MAX(decimals) AS decimals
  FROM {{ref('ethereum__token_prices_hourly')}}
  GROUP BY 1,2
)

SELECT
  s.block_timestamp,
  s.pool_address,
  COALESCE(l.address_name,lp.pool_name) AS pool_name,
  s.tx_id,
  s.event_index,
  s.swapper,
  p0.coins AS token_in,
  s.amount_in/POWER(10,COALESCE(dc0.meta:decimals,tp0.decimals)) AS amount_in,
  s.amount_in/POWER(10,COALESCE(dc0.meta:decimals,tp0.decimals))*tp0.price AS amount_in_usd,
  p1.coins AS token_out,
  s.amount_out/POWER(10,COALESCE(dc1.meta:decimals,tp1.decimals)) AS amount_out,
  s.amount_out/POWER(10,COALESCE(dc1.meta:decimals,tp1.decimals))*tp1.price AS amount_out_usd
FROM
curve_swaps_raw s
  -- Info for the pool --
LEFT JOIN
{{source('ethereum', 'ethereum_address_labels')}} l
    ON s.pool_address = l.address
LEFT JOIN
{{ref('ethereum__curve_liquidity_pools')}} lp
    ON s.pool_address = lp.pool_address
  -- Token being used by the swapper for the swap --
LEFT OUTER JOIN
pool_tokens_parsed p0
    ON s.index_in = p0.index AND s.pool_address = p0.pool_add
LEFT OUTER JOIN
prices tp0
    ON date_trunc('hour',s.block_timestamp) = tp0.hour AND p0.coins = tp0.token_address
LEFT JOIN {{source('ethereum', 'ethereum_contracts')}} dc0
    ON p0.coins = dc0.address
  -- Token the swapper is getting --
LEFT OUTER JOIN
pool_tokens_parsed p1
    ON s.index_out = p1.index AND s.pool_address = p1.pool_add
LEFT OUTER JOIN
prices tp1
    ON date_trunc('hour',s.block_timestamp) = tp1.hour AND p1.coins = tp1.token_address
LEFT JOIN {{source('ethereum', 'ethereum_contracts')}} dc1
    ON p1.coins = dc1.address
    WHERE token_in IS NOT NULL AND token_out IS NOT NULL 
    ORDER BY block_timestamp DESC
    