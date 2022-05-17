{{ config(
  materialized = 'table',
  unique_key = 'date',
  incremental_strategy = 'delete+insert',
  cluster_by = ['date'],
  tags = ['snowflake', 'mirror', 'console', 'mirror_eth_masset_supply']
) }}

-- original by FlipoCrypto https://app.flipsidecrypto.com/velocity/queries/e4e8dcf0-d629-4aa2-9140-f52006cae37a

SELECT 
  to_char(date_trunc('day', balance_date), 'YYYY-MM-DD HH24:MI:SS') AS date,
  symbol,
  sum(balance) as amount
FROM {{ ref('ethereum__erc20_balances') }}
WHERE contract_label = 'mirror' 
  AND (symbol LIKE 'm%' OR symbol = 'MIR')

{% if is_incremental() %}
  AND balance_date :: DATE >= (SELECT MAX( block_timestamp :: DATE )FROM {{ ref('silver_terra__msgs') }})
{% endif %}

GROUP BY 1,2

UNION 

SELECT 
  to_char(date_trunc('day', balance_date), 'YYYY-MM-DD HH24:MI:SS') AS date,
  'Total' symbol,
  sum(balance) as amount
FROM {{ ref('ethereum__erc20_balances') }}
WHERE contract_label = 'mirror' 
  AND (symbol LIKE 'm%' OR symbol = 'MIR')

{% if is_incremental() %}
  AND balance_date :: DATE >= (SELECT MAX( block_timestamp :: DATE )FROM {{ ref('silver_terra__msgs') }})
{% endif %}

GROUP BY 1,2