{{ config(
  materialized = 'table',
  unique_key = 'date',
  incremental_strategy = 'delete+insert',
  cluster_by = ['date'],
  tags = ['snowflake', 'mirror', 'console', 'mirror_eth_total_tx_fee']
) }}

WITH token_symbols as (
  
  SELECT 
    DISTINCT symbol, 
    contract_address
  FROM ethereum.erc20_balances
  WHERE contract_label = 'mirror' 
    AND (symbol LIKE 'm%' OR symbol = 'MIR') 
    AND user_address != '0x7a250d5630b4cf539739df2c5dacb4c659f2488d'
    AND balance_date >= CURRENT_DATE - 30
  
)

, tx as(
  
  SELECT 
    tx_id, 
    s.symbol
  FROM ethereum.udm_events u 
  
  LEFT JOIN token_symbols s 
    ON u.contract_address = s.contract_address
  
  WHERE u.contract_address IN (select contract_address from token_symbols)
  AND u.block_timestamp >= CURRENT_DATE - 180

)


SELECT 
  to_char(date_trunc('day', block_timestamp), 'YYYY-MM-DD HH24:MI:SS') AS date, 
  tx.symbol, 
  sum(fee_usd) as fee_usd
FROM ethereum.transactions

JOIN tx 
  ON transactions.tx_id = tx.tx_id

WHERE tx.symbol is not null
  AND transactions.block_timestamp >= CURRENT_DATE - 180
GROUP BY 1,2

UNION

SELECT 
  to_char(date_trunc('day', block_timestamp), 'YYYY-MM-DD HH24:MI:SS') AS date, 
  'Total' as symbol, 
  sum(fee_usd) as fee_usd
FROM ethereum.transactions

JOIN tx 
  ON transactions.tx_id = tx.tx_id

WHERE tx.symbol is not null
  AND transactions.block_timestamp >= CURRENT_DATE - 180
GROUP BY 1,2
