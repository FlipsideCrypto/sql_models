{{ config(
  materialized = 'table',
  unique_key = 'date',
  incremental_strategy = 'delete+insert',
  cluster_by = ['date'],
  tags = ['snowflake', 'mirror', 'console', 'mirror_eth_masset_prices']
) }}

-- original by FlipoCrypto https://app.flipsidecrypto.com/velocity/queries/cf6d38d9-2a4f-468a-992e-ed96b790e696

SELECT 
  to_char(date_trunc('day', hour), 'YYYY-MM-DD HH24:MI:SS') AS "DATE",
  token_address,
  symbol,
  avg(price) AS metric_value
FROM {{ ref('ethereum__token_prices_hourly') }}
WHERE symbol IN (
	SELECT symbol
	FROM {{ ref('ethereum__erc20_balances') }}
    WHERE contract_label = 'mirror' AND label = 'uniswap' AND balance_date >= CURRENT_DATE - 90
      AND user_address != '0x7a250d5630b4cf539739df2c5dacb4c659f2488d'
  ) 
  AND hour >= CURRENT_DATE - 90
  
  {% if is_incremental() %}
    AND hour :: DATE >= (SELECT MAX( block_timestamp :: DATE )FROM {{ ref('silver_terra__msgs') }})
  {% endif %}

GROUP BY 1,2,3