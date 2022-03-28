{{ config(
  materialized = 'table',
  unique_key = 'date',
  incremental_strategy = 'delete+insert',
  cluster_by = ['date'],
  tags = ['snowflake', 'mirror', 'console', 'mirror_eth_masset_tvl']
) }}


-- original by FlipoCrypto https://app.flipsidecrypto.com/velocity/queries/5eb33bd0-3c54-47d8-9393-ec5ff186b08d

WITH label as (

  SELECT user_address, symbol
  FROM ethereum.erc20_balances
  WHERE contract_label = 'mirror' 
    AND label = 'uniswap' 
    AND balance_date >= CURRENT_DATE - 30 
    AND user_address != '0x7a250d5630b4cf539739df2c5dacb4c659f2488d'
 
 )

, balance_usd as (
  
  SELECT 
    balance_date, 
    user_address,
    CASE
      WHEN symbol = 'UST' then balance
      ELSE amount_usd
    END as amount_usd
  FROM ethereum.erc20_balances
  WHERE user_address IN (SELECT user_address FROM label)

)

, liquidity_contract as (

  SELECT 
    balance_date,
    user_address,
    sum(amount_usd) as liquidity
  FROM balance_usd
  GROUP BY 1, 2

)

, liquidity_masset as (

  SELECT 
    DISTINCT balance_date,
    symbol,
    liquidity
  FROM liquidity_contract c 
  
  JOIN label l 
    ON c.user_address = l.user_address

)

SELECT 
  to_char(date_trunc('day', balance_date), 'YYYY-MM-DD HH24:MI:SS') AS date,
  symbol,
  sum(liquidity) as tvl
FROM liquidity_masset
GROUP BY 1,2

UNION

SELECT 
  to_char(date_trunc('day', balance_date), 'YYYY-MM-DD HH24:MI:SS') AS date,
  'Total' as symbol,
  sum(liquidity) as tvl
FROM liquidity_masset
GROUP BY 1,2
