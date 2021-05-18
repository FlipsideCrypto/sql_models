{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='date || address',
    incremental_strategy='delete+insert',
    cluster_by=['date', 'address'],
    tags=['snowflake', 'algorand', 'events']
  )
}}

-- Get the average token price per hour
WITH prices AS (
  SELECT
    p.symbol,
    date_trunc('day', recorded_at) as day,
    avg(price) as price
  FROM {{ source('shared', 'prices')}} p
  WHERE
    p.asset_id = 4030
    {% if is_incremental() %}
      AND recorded_at >= getdate() - interval '7 days'
    {% else %}
      AND recorded_at >= getdate() - interval '9 months'
    {% endif %}
  GROUP BY p.symbol, day
),

balances AS (
  SELECT   
    b.address as address,
    labels.address_name as address_name,
    labels.project_name as address_label,
    labels.l1_label as address_label_type,
    labels.l2_label as address_label_subtype,
    -- prices.price,
    b.balance,
    b.balance_type,
    -- Value of the token in USD
    prices.price * balance as balance_usd,
    b.currency,
    date
  FROM 
    -- join against the clean daily balances table
    {{ source('algorand', 'udm_daily_balances_algorand') }} b

  LEFT OUTER JOIN
    -- Labels for addresses
    {{ source('shared', 'udm_address_labels_new') }} as labels
      ON b.address = labels.address

  LEFT OUTER JOIN
    prices
      ON prices.symbol = b.currency
        AND date_trunc('day', b.date) = prices.day

  WHERE
    {% if is_incremental() %}
        date >= getdate() - interval '7 days'
    {% else %}
        date >= getdate() - interval '9 months'
    {% endif %}
)
SELECT * FROM balances ORDER BY date DESC 