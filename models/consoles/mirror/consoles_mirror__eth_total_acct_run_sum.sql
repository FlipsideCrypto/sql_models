{{ config(
  materialized = 'table',
  unique_key = 'date',
  incremental_strategy = 'delete+insert',
  cluster_by = ['date'],
  tags = ['snowflake', 'mirror', 'console', 'mirror_eth_total_acct']
) }}

-- original by FlipoCrypto https://app.flipsidecrypto.com/velocity/queries/a3a047c9-b0e8-4fd9-a712-a0a716a4e095

WITH token_symbols as (
  
  SELECT 
    DISTINCT symbol, 
    contract_address
  FROM {{ ref('ethereum__erc20_balances') }}
  WHERE contract_label = 'mirror' 
    AND (symbol LIKE 'm%' OR symbol = 'MIR')
    AND balance_date >= CURRENT_DATE - 30

)

, from_to_addresses as (
  
  SELECT 
    to_address AS address, --TO address
    s.symbol,
    min(block_timestamp) AS start_date
  FROM {{ ref('ethereum__udm_events') }} u 
  
  LEFT JOIN token_symbols s 
    ON u.contract_address = s.contract_address
  
  WHERE u.contract_address IN (select contract_address from token_symbols)
  
  {% if is_incremental() %}
    AND u.block_timestamp :: DATE >= (SELECT MAX( block_timestamp :: DATE )FROM {{ ref('silver_terra__msgs') }})
  {% endif %}
  
  GROUP BY 1,2
  
  UNION
  
  SELECT 
    from_address AS address, --FROM address
    s.symbol,
    min(block_timestamp) AS start_date
  FROM {{ ref('ethereum__udm_events') }} u 
  
  LEFT JOIN token_symbols s  
    ON u.contract_address = s.contract_address
  
  WHERE u.contract_address IN (select contract_address from token_symbols)
  
  {% if is_incremental() %}
    AND u.block_timestamp :: DATE >= (SELECT MAX( block_timestamp :: DATE )FROM {{ ref('silver_terra__msgs') }})
  {% endif %}
  
  GROUP BY 1,2

)

, address_start_date as (
  	
    SELECT 
      address, 
      symbol, 
      min(start_date) AS start_date
  	FROM from_to_addresses
  	GROUP BY 1, 2

)

, num_address_day_symbol as (

  SELECT 
    to_char(date_trunc('day', start_date), 'YYYY-MM-DD HH24:MI:SS') AS date, 
    symbol, 
    count(distinct address) AS address
  FROM address_start_date
  GROUP BY 1,2

  UNION 

  SELECT 
    to_char(date_trunc('day', start_date), 'YYYY-MM-DD HH24:MI:SS') AS date, 
    'Total' as symbol, 
    count(distinct address) AS address
  FROM address_start_date
  GROUP BY 1,2

)

SELECT 
  date, 
  symbol, 
  sum(address) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum
FROM num_address_day_symbol

