{{ config(
  materialized = 'view',
  unique_key = "CONCAT_WS('-', metric_date, currency, segment)",
  tags = ['snowflake', 'console', 'terra', 'token_breakdown']
) }}


with recent_events as 
  (
select *
from  {{ ref('terra__transfers') }}
where block_timestamp >= CURRENT_DATE - 180
),

krt_outflow as (

SELECT block_timestamp::date as metric_date,
       event_currency,
       CASE
           WHEN event_from_label_type = 'operator' THEN 'Operator'
           WHEN event_from IN('terra1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8pm7utl','terra1dp0taj85ruc299rkdvzp4z5pfg6z6swaed74e6') THEN 'Foundation'
           WHEN event_from_label_type IN('distributor','cex') THEN 'Exchanges'
           WHEN event_from IN(SELECT address FROM {{ ref('consoles_terra__top_holders') }} WHERE currency = 'KRT') AND event_from_label_type IS NULL THEN 'Top Holder'
           ELSE 'Smaller Wallets'
       END AS segment,
       sum(event_amount) AS outflow
FROM recent_events
WHERE event_currency = 'KRT'
GROUP BY metric_date, event_currency, segment
),

krt_inflow as (
  
SELECT block_timestamp::date as metric_date,
       event_currency,
       CASE
           WHEN event_to_label_type = 'operator' THEN 'Operator'
           WHEN event_to IN('terra1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8pm7utl','terra1dp0taj85ruc299rkdvzp4z5pfg6z6swaed74e6') THEN 'Foundation'
           WHEN event_to_label_type IN('distributor','cex') THEN 'Exchanges'
           WHEN event_to IN(SELECT address FROM {{ ref('consoles_terra__top_holders') }} WHERE currency = 'KRT') AND event_to_label_type IS NULL THEN 'Top Holder'
           ELSE 'Smaller Wallets'
       END AS segment,
       sum(event_amount) AS inflow
FROM recent_events
WHERE event_currency = 'KRT'
GROUP BY metric_date, event_currency, segment
),

luna_outflow as (

SELECT block_timestamp::date as metric_date,
       event_currency,
       CASE
           WHEN event_from_label_type = 'operator' THEN 'Operator'
           WHEN event_from IN('terra1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8pm7utl','terra1dp0taj85ruc299rkdvzp4z5pfg6z6swaed74e6') THEN 'Foundation'
           WHEN event_from_label_type IN('distributor','cex') THEN 'Exchanges'
           WHEN event_from IN(SELECT address FROM {{ ref('consoles_terra__top_holders') }} WHERE currency = 'LUNA') AND event_from_label_type IS NULL THEN 'Top Holder'
           ELSE 'Smaller Wallets'
       END AS segment,
       sum(event_amount) AS outflow
FROM recent_events
WHERE event_currency = 'LUNA'
GROUP BY metric_date, event_currency, segment
),

luna_inflow as (
SELECT block_timestamp::date as metric_date,
       event_currency,
       CASE
           WHEN event_to_label_type = 'operator' THEN 'Operator'
           WHEN event_to IN('terra1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8pm7utl','terra1dp0taj85ruc299rkdvzp4z5pfg6z6swaed74e6') THEN 'Foundation'
           WHEN event_to_label_type IN('distributor','cex') THEN 'Exchanges'
           WHEN event_to IN(SELECT address FROM {{ ref('consoles_terra__top_holders') }} WHERE currency = 'LUNA') AND event_to_label_type IS NULL THEN 'Top Holder'
           ELSE 'Smaller Wallets'
       END AS segment,
       sum(event_amount) AS inflow
FROM recent_events
WHERE event_currency = 'LUNA'
GROUP BY metric_date, event_currency, segment
),
 
sdt_outflow as (

SELECT block_timestamp::date as metric_date,
       event_currency,
       CASE
           WHEN event_from_label_type = 'operator' THEN 'Operator'
           WHEN event_from IN('terra1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8pm7utl','terra1dp0taj85ruc299rkdvzp4z5pfg6z6swaed74e6') THEN 'Foundation'
           WHEN event_from_label_type IN('distributor','cex') THEN 'Exchanges'
           WHEN event_from IN(SELECT address FROM {{ ref('consoles_terra__top_holders') }} WHERE currency = 'SDT') AND event_from_label_type IS NULL THEN 'Top Holder'
           ELSE 'Smaller Wallets'
       END AS segment,
       sum(event_amount) AS outflow
FROM recent_events
WHERE event_currency = 'SDT'
GROUP BY metric_date, event_currency, segment
),

sdt_inflow as (
SELECT block_timestamp::date as metric_date,
       event_currency,
       CASE
           WHEN event_to_label_type = 'operator' THEN 'Operator'
           WHEN event_to IN('terra1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8pm7utl','terra1dp0taj85ruc299rkdvzp4z5pfg6z6swaed74e6') THEN 'Foundation'
           WHEN event_to_label_type IN('distributor','cex') THEN 'Exchanges'
           WHEN event_to IN(SELECT address FROM {{ ref('consoles_terra__top_holders') }} WHERE currency = 'SDT') AND event_to_label_type IS NULL THEN 'Top Holder'
           ELSE 'Smaller Wallets'
       END AS segment,
       sum(event_amount) AS inflow
FROM recent_events
WHERE event_currency = 'SDT'
GROUP BY metric_date, event_currency, segment
),

ust_outflow as (

SELECT block_timestamp::date as metric_date,
       event_currency,
       CASE
           WHEN event_from_label_type = 'operator' THEN 'Operator'
           WHEN event_from IN('terra1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8pm7utl','terra1dp0taj85ruc299rkdvzp4z5pfg6z6swaed74e6') THEN 'Foundation'
           WHEN event_from_label_type IN('distributor','cex') THEN 'Exchanges'
           WHEN event_from IN(SELECT address FROM {{ ref('consoles_terra__top_holders') }} WHERE currency = 'UST') AND event_from_label_type IS NULL THEN 'Top Holder'
           ELSE 'Smaller Wallets'
       END AS segment,
       sum(event_amount) AS outflow
FROM recent_events
WHERE event_currency = 'UST'
GROUP BY metric_date, event_currency, segment
),

ust_inflow as (
SELECT block_timestamp::date as metric_date,
       event_currency,
       CASE
           WHEN event_to_label_type = 'operator' THEN 'Operator'
           WHEN event_to IN('terra1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8pm7utl','terra1dp0taj85ruc299rkdvzp4z5pfg6z6swaed74e6') THEN 'Foundation'
           WHEN event_to_label_type IN('distributor','cex') THEN 'Exchanges'
           WHEN event_to IN(SELECT address FROM {{ ref('consoles_terra__top_holders') }} WHERE currency = 'UST') AND event_to_label_type IS NULL THEN 'Top Holder'
           ELSE 'Smaller Wallets'
       END AS segment,
       sum(event_amount) AS inflow
FROM recent_events
WHERE event_currency = 'UST'
GROUP BY metric_date, event_currency, segment
),

combine as (
 
 select 
  coalesce (i.metric_date,o.metric_date) as metric_date,
  coalesce(i.event_currency,o.event_currency) as currency,
  coalesce (i.segment, o.segment) as segment,
  zeroifnull(inflow) as inflow,
  zeroifnull(outflow) as outflow,
  zeroifnull(inflow) - zeroifnull(outflow) as daily_change
  from ust_inflow i
  full outer join ust_outflow o
  on o.metric_date = i.metric_date
  and o.event_currency = i.event_currency
  and o.segment = i.segment 

  UNION 
  
  select 
  coalesce (i.metric_date,o.metric_date) as metric_date,
  coalesce(i.event_currency,o.event_currency) as currency,
  coalesce (i.segment, o.segment) as segment,
  zeroifnull(inflow) as inflow,
  zeroifnull(outflow) as outflow,
  zeroifnull(inflow) - zeroifnull(outflow) as daily_change
  from sdt_inflow i
  full outer join sdt_outflow o
  on o.metric_date = i.metric_date
  and o.event_currency = i.event_currency
  and o.segment = i.segment 
 
UNION 
 
  select 
  coalesce (i.metric_date,o.metric_date) as metric_date,
  coalesce(i.event_currency,o.event_currency) as currency,
  coalesce (i.segment, o.segment) as segment,
  zeroifnull(inflow) as inflow,
  zeroifnull(outflow) as outflow,
  zeroifnull(inflow) - zeroifnull(outflow) as daily_change
  from krt_inflow i
  full outer join krt_outflow o
  on o.metric_date = i.metric_date
  and o.event_currency = i.event_currency
  and o.segment = i.segment
 
 UNION
 
  select 
  coalesce (i.metric_date,o.metric_date) as metric_date,
  coalesce(i.event_currency,o.event_currency) as currency,
  coalesce (i.segment, o.segment) as segment,
  zeroifnull(inflow) as inflow,
  zeroifnull(outflow) as outflow,
  zeroifnull(inflow) - zeroifnull(outflow) as daily_change
  from luna_inflow i
  full outer join luna_outflow o
  on o.metric_date = i.metric_date
  and o.event_currency = i.event_currency
  and o.segment = i.segment
  )
  
  select *
  from combine
  order by metric_date desc