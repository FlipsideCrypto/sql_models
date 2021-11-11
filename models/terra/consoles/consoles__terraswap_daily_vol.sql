{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_date, swap_value_usd)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_date'],
  tags = ['snowflake', 'terra', 'terraswap', 'console']
) }}


WITH prices AS
(
  SELECT
      DATE(block_timestamp) block_date,
      currency,
      AVG(PRICE_USD) avg_price_usd
    FROM {{ ref('terra__oracle_prices') }} 
     where block_timestamp > CURRENT_DATE - interval '90 days'
    GROUP BY 1, 2
)
,

swaps AS
(
  SELECT 
    -- m.block_id,
  	DATE_TRUNC('week', m.block_timestamp) block_week,
  	DATE_TRUNC('month', m.block_timestamp) block_month,
    DATE(m.block_timestamp) block_date,
    m.block_timestamp,
    m.tx_id,
    msg_value:sender::string as sender,
    -- msg_value:execute_msg:swap:assets[0]:amount / POW(10,6) as token_0_amount,
    -- msg_value:execute_msg:provide_liquidity:assets[0]:info:token:contract_addr::string as token_0_address,
    -- t0.address_name as token_0_address_name,
    -- msg_value:execute_msg:provide_liquidity:assets[1]:amount / POW(10,6) as token_1_amount,
    -- msg_value:execute_msg:provide_liquidity:assets[1]:info:native_token:denom::string as token_1_address,
    msg_value:coins[0]:amount / POW(10,6) offered_amount,
    msg_value:coins[0]:denom::string offered_denom,
    (msg_value:coins[0]:amount / POW(10,6)) * p.avg_price_usd swap_usd_value,
    -- t1.symbol as token_1_address_name,
    -- msg_value:contract::string as contract_address,
    -- msg_value,
    SUBSTR(c.address, 11, LEN(c.address)) as contract_label -- Take out the "TerraSwap in the beginning"
  FROM {{ ref('terra__msgs') }} m
  -- LEFT OUTER JOIN terra.labels t0
  --   ON msg_value:execute_msg:provide_liquidity:assets[0]:info:token:contract_addr = t0.address
    
  -- LEFT OUTER JOIN prices t1
  --   ON msg_value:execute_msg:provide_liquidity:assets[1]:info:native_token:denom::string  = t1.currency
  
  LEFT OUTER JOIN {{ ref('terra__labels') }} c -- is this table name correct? 
    ON msg_value:contract  = c.address
  JOIN prices p
    ON DATE(m.block_timestamp) = p.block_date AND msg_value:coins[0]:denom::string = p.currency
  
  WHERE 
  --msg_value:contract IN(SELECT address FROM terra.labels WHERE label = 'terraswap' AND label_subtype ='pool') --All TerraSwap Pools 
    msg_value:execute_msg:swap IS NOT NULL --Ensures we only look swaps actions in terraswap
    AND m.block_timestamp >= CURRENT_DATE - 90
    AND m.TX_STATUS = 'SUCCEEDED'
  ORDER BY m.block_timestamp DESC
  -- LIMIT 100
)

SELECT
  block_date,
  --block_week,
  --block_month,
  --contract_label,
  SUM(swap_usd_value) swap_value_usd
FROM
  swaps
GROUP BY 1
  --,2,3,4
ORDER BY 1 DESC