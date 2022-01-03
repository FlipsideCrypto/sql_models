{{ config(
  materialized = 'incremental',
  sort = ['date', 'currency'],
  unique_key = "CONCAT_WS('-', date, address, currency, balance_type)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['date'],
  tags = ['snowflake', 'terra', 'balances', 'terra_daily_synthetic_balances', 'address_labels']
) }}

WITH prices AS (

  SELECT
    p.symbol,
    DATE_TRUNC('day', block_timestamp) AS DAY,
    AVG(price_usd) AS price
  FROM
    {{ ref('terra__oracle_prices') }} p
  GROUP BY
    p.symbol,
    DAY

),

rn as (

SELECT 
    block_timestamp,
    address,
    currency,
    balance,
    row_number() OVER (PARTITION BY date_trunc('day', block_timestamp), address, currency, chain_id ORDER BY block_timestamp DESC, block_id DESC) as rn
FROM {{ ref('silver_terra__block_synthetic_balances') }}

{% if is_incremental() %}
AND block_timestamp :: DATE >= ( SELECT DATEADD('day', -1, MAX(system_created_at :: DATE)) FROM {{ this }})
{% endif %}

)

SELECT
  date_trunc('day', b.block_timestamp) as date,
  b.address,
  address_labels.l1_label AS address_label_type,
  address_labels.l2_label AS address_label_subtype,
  address_labels.project_name AS address_label,
  address_labels.address_name AS address_name,
  balance,
  balance * p.price AS balance_usd,
  currency
FROM rn b
  
LEFT OUTER JOIN prices p
  ON p.symbol = currency
  AND p.day = date_trunc('day', b.block_timestamp)
  
LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS address_labels
  ON b.address = address_labels.address 
  AND address_labels.blockchain = 'terra' 
  AND address_labels.creator = 'flipside'

WHERE rn = 1
  AND balance > 0

{% if is_incremental() %}
AND date_trunc('day', b.block_timestamp) >= getdate() - INTERVAL '3 days'
{% endif %}