{{ config(
  materialized = 'view',
  unique_key = 'block_date',
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
  	DATE_TRUNC('week', m.block_timestamp) block_week,
  	DATE_TRUNC('month', m.block_timestamp) block_month,
    DATE(m.block_timestamp) block_date,
    m.block_timestamp,
    m.tx_id,
    msg_value:sender::string as sender,
    msg_value:coins[0]:amount / POW(10,6) offered_amount,
    msg_value:coins[0]:denom::string offered_denom,
    (msg_value:coins[0]:amount / POW(10,6)) * p.avg_price_usd swap_usd_value,
    SUBSTR(c.address, 11, LEN(c.address)) as contract_label 
  FROM {{ ref('terra__msgs') }} m
  LEFT OUTER JOIN {{ ref('terra__labels') }} c 
    ON msg_value:contract  = c.address
  JOIN prices p
    ON DATE(m.block_timestamp) = p.block_date AND msg_value:coins[0]:denom::string = p.currency
  
  WHERE 
      msg_value:execute_msg:swap IS NOT NULL 
    AND m.block_timestamp >= CURRENT_DATE - 90
    AND m.TX_STATUS = 'SUCCEEDED'
     ORDER BY m.block_timestamp DESC
)

SELECT
  block_date,
  SUM(swap_usd_value) swap_value_usd
FROM
  swaps
GROUP BY 1
ORDER BY 1 DESC