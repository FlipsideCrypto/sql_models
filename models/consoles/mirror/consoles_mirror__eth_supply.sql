{{ config(
  materialized = 'table',
  unique_key = 'date',
  incremental_strategy = 'delete+insert',
  cluster_by = ['date'],
  tags = ['snowflake', 'mirror', 'console', 'mirror_eth_new_acct']
) }}

-- original by FlipoCrypto https://app.flipsidecrypto.com/velocity/queries/3e05f1c4-cc46-4d76-910f-d5ae9ad6033c

SELECT 
  to_char(date_trunc('day', balance_date), 'YYYY-MM-DD HH24:MI:SS') AS date,
  CASE
    WHEN label = 'uniswap' THEN 'Uniswap'
    WHEN label = 'coinone' THEN 'CoinOne'
    WHEN label like 'binance%' THEN 'Binance'
    WHEN label = 'huobi' THEN 'Huobi'
    WHEN label = 'kucoin' THEN 'KuCoin'
    WHEN label IN ('crypto.com', 'crypto com') THEN 'Crypto.com'
    WHEN label = 'gemini' THEN 'Gemini'
    WHEN label = 'kraken' THEN 'Kraken'
  	WHEN label = 'coinex' THEN 'Coinex'
  	WHEN label = 'hotbit' THEN 'Hotbit'
  	WHEN label = 'gate.io' THEN 'Gate.io'
  	WHEN label = 'coinbase' THEN 'Coinbase'
  	WHEN label = 'upbit' THEN 'Upbit'
  	WHEN label = 'okex' THEN 'OKex'
    WHEN label_type = 'cex' THEN 'Other Exchanges'
  	WHEN label_type IN ('dex', 'defi') THEN 'DEX and DeFi Pools'
    ELSE 'Smaller Wallets'
  END as label,
 sum(balance) as amount
FROM ethereum.erc20_balances
WHERE contract_address = '0x09a3ecafa817268f77be1283176b946c4ff2e608' 
  AND balance_date = CURRENT_DATE - 1
GROUP BY 1,2
HAVING amount > 0
