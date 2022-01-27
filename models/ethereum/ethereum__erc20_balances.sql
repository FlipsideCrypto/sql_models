{{ 
  config(
    materialized='incremental',
    unique_key='balance_date', 
    incremental_strategy='delete+insert',
    cluster_by=['balance_date'],
    tags=['snowflake', 'ethereum', 'balances', 'erc20_balances']
  )
}}

-- Get the average token price per day
WITH token_prices AS (
  SELECT
    date_trunc('day', recorded_at) as day,
    case when p.symbol = 'ETH' then 'ETH' else lower(a.token_address) end as token_address,
    avg(price) as price
  FROM
    {{ source('shared', 'prices_v2')}} p
  JOIN
    {{ source('shared', 'market_asset_metadata')}} a
      ON p.asset_id::string = a.asset_id::string
  WHERE
    (a.platform_id = '1027' or a.asset_id = '1027' or a.platform_id = 'ethereum')

   {% if is_incremental() %}
     AND recorded_at >= getdate() - interval '2 days'
   {% endif %}
  GROUP BY 1,2
), base_table as (
  select date,
  address,
  contract_address,
  balance
  from {{ ref('silver_ethereum__daily_balances') }}
  where 1=1
  {% if is_incremental() %}
     AND date >= getdate() - interval '2 days'
   {% endif %}
), symbols as (
  select token_address
  from {{ source('shared', 'prices_v2')}} p
  join {{ source('shared', 'market_asset_metadata')}} a
      ON p.asset_id::string = a.asset_id::string
  WHERE
    (a.platform_id = '1027' or a.asset_id = '1027' or a.platform_id = 'ethereum')
    {% if is_incremental() %}
     AND recorded_at >= getdate() - interval '2 days'
   {% endif %}
  group by 1
)

  SELECT   
    date  as balance_date,
    b.address as user_address,
    labels.project_name as label,
    labels.address_name as address_name,
    labels.l1_label as label_type,
    labels.l2_label as label_subtype,
    b.contract_address,
    contract_labels.project_name as contract_label,
    CASE WHEN b.contract_address = 'ETH'
    THEN 'ETH'
    ELSE c.meta:symbol::string END as symbol,
    token_prices.price,
    c.meta:decimals as decimals,
    b.balance as non_adjusted_balance,
    CASE WHEN b.contract_address = 'ETH'
    THEN b.balance / pow(10,18)
    WHEN b.balance IS NOT NULL AND c.meta:decimals::int IS NOT NULL
    THEN b.balance / pow(10, c.meta:decimals::int)
    WHEN c.meta:symbol::string is not null and c.meta:decimals::int is null
    THEN b.balance
    ELSE NULL
END as balance,
    -- Value of the token in USD
    token_prices.price * CASE WHEN b.contract_address = 'ETH'
    THEN b.balance / pow(10,18)
    WHEN b.balance IS NOT NULL AND c.meta:decimals::int IS NOT NULL
    THEN b.balance / pow(10, c.meta:decimals::int)
    WHEN c.meta:symbol::string is not null and c.meta:decimals::int is null
    THEN b.balance
    ELSE NULL
    END as amount_usd,
    CASE WHEN s.token_address IS NOT NULL
    THEN TRUE 
    ELSE FALSE END as has_price,
    CASE WHEN c.meta:symbol::string is not null
    THEN TRUE
    ELSE FALSE END as has_decimal
  FROM 
  -- join against the clean daily balances table
  base_table b

  LEFT JOIN {{ ref('silver_ethereum__contracts')}} c
      on b.contract_address = c.address

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
        AND date_trunc('day', b.date) = token_prices.day
  
  LEFT JOIN
    symbols s
      ON s.token_address = b.contract_address

  WHERE b.balance <> 0