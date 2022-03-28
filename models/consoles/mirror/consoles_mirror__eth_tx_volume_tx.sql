{{ config(
  materialized = 'table',
  unique_key = 'date',
  incremental_strategy = 'delete+insert',
  cluster_by = ['date'],
  tags = ['snowflake', 'mirror', 'console', 'mirror_eth_total_tx_fee']
) }}

-- original by FlipoCrypto https://app.flipsidecrypto.com/velocity/queries/fe07c93f-e9ef-453b-88a6-f9312d8e6a84

WITH token_symbols as (
  SELECT 
    DISTINCT symbol, 
    contract_address
  FROM ethereum.erc20_balances
  WHERE contract_label = 'mirror' 
    AND (symbol LIKE 'm%' OR symbol = 'MIR')
    AND balance_date >= CURRENT_DATE - 30 AND user_address != '0x7a250d5630b4cf539739df2c5dacb4c659f2488d'
  
)

SELECT 
  to_char(date_trunc('day', block_timestamp), 'YYYY-MM-DD HH24:MI:SS') AS date, --TO address
  s.symbol,
  count(DISTINCT tx_id) AS tx_count
FROM ethereum.udm_events u 

LEFT JOIN token_symbols s 
  ON u.contract_address = s.contract_address

WHERE u.contract_address IN (select contract_address from token_symbols)
GROUP BY 1,2
  
UNION 
  
SELECT to_char(date_trunc('day', block_timestamp), 'YYYY-MM-DD HH24:MI:SS') AS date, --TO address
  'Total' as symbol,
count(DISTINCT tx_id) AS tx_count
FROM ethereum.udm_events
WHERE contract_address IN (select contract_address from token_symbols)
GROUP BY 1,2
ORDER BY 1

