{{ config(
  materialized = 'table',
  unique_key = 'date',
  incremental_strategy = 'delete+insert',
  cluster_by = ['date'],
  tags = ['snowflake', 'mirror', 'console', 'mirror_eth_avg_tx_fee']
) }}


-- original by FlipoCrypto https://app.flipsidecrypto.com/velocity/queries/20268753-fe3b-460f-9b14-4cf0050ebcd4
WITH token_symbols as (
  
  SELECT 
    DISTINCT symbol, 
    contract_address
  FROM {{ ref('ethereum__erc20_balances') }}
  WHERE contract_label = 'mirror' 
    AND (symbol LIKE 'm%' OR symbol = 'MIR') 
    AND user_address != '0x7a250d5630b4cf539739df2c5dacb4c659f2488d'
    AND balance_date >= CURRENT_DATE - 30

)

, tx as(
  
  SELECT 
    tx_id, 
    s.symbol
  FROM {{ ref('ethereum__udm_events') }} u 
  
  LEFT JOIN token_symbols s 
    ON u.contract_address = s.contract_address
  
  WHERE u.contract_address IN (SELECT contract_address FROM token_symbols)

  {% if is_incremental() %}
    AND u.block_timestamp :: DATE >= (SELECT MAX( block_timestamp :: DATE )FROM {{ ref('silver_terra__msgs') }})
  {% endif %}

)


SELECT 
  to_char(date_trunc('day', block_timestamp), 'YYYY-MM-DD HH24:MI:SS') AS date, 
  tx.symbol, 
  avg(fee_usd) as fee_usd
FROM {{ ref('ethereum__transactions') }} m

JOIN tx 
  ON m.tx_id = tx.tx_id

WHERE tx.symbol IS NOT NULL

{% if is_incremental() %}
  AND m.block_timestamp :: DATE >= (SELECT MAX( block_timestamp :: DATE )FROM {{ ref('silver_terra__msgs') }})
{% endif %}

GROUP BY 1,2

UNION

SELECT 
  to_char(date_trunc('day', block_timestamp), 'YYYY-MM-DD HH24:MI:SS') AS date, 
  'Total' as symbol, 
  avg(fee_usd) as fee_usd
FROM {{ ref('ethereum__transactions') }} m

JOIN tx 
  ON m.tx_id = tx.tx_id

WHERE tx.symbol IS NOT NULL

{% if is_incremental() %}
  AND m.block_timestamp :: DATE >= (SELECT MAX( block_timestamp :: DATE )FROM {{ ref('silver_terra__msgs') }})
{% endif %}

GROUP BY 1,2
