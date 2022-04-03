{{ config(
  materialized = 'table',
  unique_key = 'date',
  incremental_strategy = 'delete+insert',
  cluster_by = ['date'],
  tags = ['snowflake', 'mirror', 'console', 'mirror_eth_daily_active_wallets']
) }}

-- original by FlipoCrypto https://app.flipsidecrypto.com/velocity/queries/e633b19b-8fd2-4989-b8c7-3637bb4488a9

WITH token_symbols as (
  
  SELECT 
    DISTINCT symbol, 
    contract_address
  FROM {{ ref('ethereum__erc20_balances') }}
  WHERE contract_label = 'mirror' 
    AND (symbol LIKE 'm%' OR symbol = 'MIR')
    AND balance_date >= CURRENT_DATE - 30
    
)

, joined as (
  
  SELECT 
    to_char(date_trunc('day', block_timestamp), 'YYYY-MM-DD HH24:MI:SS') AS date,
    s.symbol,
    count(distinct tx_from_address) AS value
  FROM {{ ref('ethereum__events_emitted') }} e 
  
  LEFT JOIN token_symbols s 
    ON e.contract_address = s.contract_address
  
  WHERE e.contract_address IN (select contract_address from token_symbols)
  
  {% if is_incremental() %}
    AND e.block_timestamp :: DATE >= (SELECT MAX( block_timestamp :: DATE )FROM {{ ref('silver_terra__msgs') }})
  {% endif %}
  
  GROUP BY 1,2
  
  UNION
  
  SELECT 
    to_char(date_trunc('day', block_timestamp), 'YYYY-MM-DD HH24:MI:SS') AS date,
    'Total' as symbol,
    count(distinct tx_from_address) AS value
  FROM {{ ref('ethereum__events_emitted') }} 
  WHERE	contract_address IN (SELECT contract_address 
                             FROM token_symbols)
  
  {% if is_incremental() %}
    AND block_timestamp :: DATE >= (SELECT MAX( block_timestamp :: DATE )FROM {{ ref('silver_terra__msgs') }})
  {% endif %}
  
  GROUP BY 1,2

)

SELECT 
  date, 
  symbol, 
  SUM(value) as value
FROM joined
GROUP BY 1, 2