{{ 
  config(
    materialized='incremental', 
    sort=['balance_date', 'symbol'], 
    unique_key='balance_date || user_address', 
    incremental_strategy='delete+insert',
    cluster_by=['balance_date', 'user_address', 'contract_address'],
    tags=['snowflake', 'ethereum', 'balances', 'erc20_balances', 'address_labels']
  )
}}

-- Get the average token price per day
WITH token_prices AS (
  SELECT
    p.symbol,
    date_trunc('day', recorded_at) as day,
    lower(a.token_address) as token_address,
    avg(price) as price
  FROM
    {{ source('shared', 'prices')}} p
  JOIN
    {{ source('shared', 'cmc_assets')}} a
      ON p.asset_id = a.asset_id
  WHERE
    a.platform_id = 1027

   {% if is_incremental() %}
     AND recorded_at >= getdate() - interval '1 days'
   {% else %}
     AND recorded_at >= getdate() - interval '9 months'
   {% endif %}
  GROUP BY p.symbol, day, token_address
), 

balances AS (
  SELECT   
    balance_date,
    b.address as user_address,
    labels.project_name as label,
    labels.address_name as address_name,
    labels.l1_label as label_type,
    labels.l2_label as label_subtype,
    b.contract_address,
    contract_labels.project_name as contract_label,
    b.symbol,
    token_prices.price,
    b.balance,
    -- Value of the token in USD
    token_prices.price * balance as amount_usd
  FROM 
  -- join against the clean daily balances table
  ({{ safe_ethereum_balances(source('ethereum', 'daily_ethereum_token_balances'), '1 days', '9 months')}}) b

  LEFT OUTER JOIN
    -- Labels for addresses
    {{ ref('silver_crosschain__address_labels') }} as labels
      ON b.address = labels.address AND labels.blockchain = 'ethereum' AND labels.creator = 'flipside'

  LEFT OUTER JOIN
    -- Labels for contracts
    {{ ref('silver_crosschain__address_labels') }} as contract_labels
      ON contract_labels.address = b.contract_address AND contract_labels.blockchain = 'ethereum' AND contract_labels.creator = 'flipside'

  LEFT OUTER JOIN
    token_prices
      ON token_prices.token_address = b.contract_address
        AND date_trunc('day', b.balance_date) = token_prices.day

  WHERE
    {% if is_incremental() %}
        balance_date >= getdate() - interval '1 days'
    {% else %}
        balance_date >= getdate() - interval '9 months'
    {% endif %}
)
SELECT * FROM balances ORDER BY balance_date DESC