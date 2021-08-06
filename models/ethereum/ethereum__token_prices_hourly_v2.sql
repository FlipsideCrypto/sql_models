{{ 
  config(
    materialized='incremental', 
    sort='hour', 
    unique_key='hour', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'ethereum', 'events']
  )
}}


SELECT
  p.symbol,
  date_trunc('hour', recorded_at) as hour,
  lower(a.token_address) as token_address,
  d.decimals,
  avg(price) as price
FROM
  {{ source('shared', 'prices_v2')}} p
JOIN
  {{ source('shared', 'market_asset_metadata')}} a
ON
  p.asset_id = a.asset_id
LEFT OUTER JOIN
  {{ source('ethereum', 'ethereum_contract_decimal_adjustments')}} d
ON
  d.address = lower(a.token_address)
WHERE
    (a.platform_id = '1027' OR a.asset_id = '1027' or a.platform_id = 'ethereum')
   {% if is_incremental() %}
     AND recorded_at >= getdate() - interval '45 days'
   {% else %}
     AND recorded_at >= '2020-05-05'::timestamp
   {% endif %}

GROUP BY 1,2,3,4
