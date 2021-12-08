{{ config(
  materialized = 'view',
  unique_key = "CONCAT_WS('-', metric_date, currency, to_segment, from_segment)",
  tags = ['snowflake', 'console', 'terra', 'foundation_flows']
) }}

with recent_events as 
  (
select *
from {{ ref('terra__transfers') }}
where block_timestamp >= CURRENT_DATE - 360

),

foundation_flows as (
SELECT date_trunc('day', block_timestamp) AS metric_date,
       CASE
           WHEN event_from_label_type = 'operator' THEN 'Operator'
           WHEN event_from IN('terra1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8pm7utl','terra1dp0taj85ruc299rkdvzp4z5pfg6z6swaed74e6') THEN 'Foundation'
           WHEN event_from_label_type IN('distributor','cex') THEN 'Exchanges'
           WHEN event_from IN(select distinct address from {{ ref('silver_crosschain__address_tags') }} where tag_type = 'krt_top_holder' and blockchain = 'terra') AND event_from_label_type IS NULL THEN 'Top Holder'
           ELSE 'Smaller Wallets'
       END AS from_segment,
       CASE
           WHEN event_to_label_type = 'operator' THEN 'Operator'
           WHEN event_to IN('terra1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8pm7utl','terra1dp0taj85ruc299rkdvzp4z5pfg6z6swaed74e6') THEN 'Foundation'
           WHEN event_to_label_type IN('distributor','cex') THEN 'Exchanges'
           WHEN event_to IN(select distinct address from {{ ref('silver_crosschain__address_tags') }} where tag_type = 'krt_top_holder' and blockchain = 'terra') AND event_to_label_type IS NULL THEN 'Top Holder'
           ELSE 'Smaller Wallets'
       END AS to_segment,
       event_currency,
       sum(event_amount) as volume,
       count(distinct tx_id) AS tx_count,
      count(distinct event_from) AS address_count
FROM recent_events
WHERE (from_segment = 'Foundation' or to_segment = 'Foundation')
  AND from_segment != to_segment
  AND event_currency = 'KRT'
GROUP BY metric_date, from_segment, to_segment, event_currency

  
UNION

SELECT date_trunc('day', block_timestamp) AS metric_date,
       CASE
           WHEN event_from_label_type = 'operator' THEN 'Operator'
           WHEN event_from IN('terra1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8pm7utl','terra1dp0taj85ruc299rkdvzp4z5pfg6z6swaed74e6') THEN 'Foundation'
           WHEN event_from_label_type IN('distributor','cex') THEN 'Exchanges'
           WHEN event_from IN(select distinct address from {{ ref('silver_crosschain__address_tags') }} where tag_type = 'sdt_top_holder' and blockchain = 'terra') AND event_from_label_type IS NULL THEN 'Top Holder'
           ELSE 'Smaller Wallets'
       END AS from_segment,
       CASE
           WHEN event_to_label_type = 'operator' THEN 'Operator'
           WHEN event_to IN('terra1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8pm7utl','terra1dp0taj85ruc299rkdvzp4z5pfg6z6swaed74e6') THEN 'Foundation'
           WHEN event_to_label_type IN('distributor','cex') THEN 'Exchanges'
           WHEN event_to IN(select distinct address from {{ ref('silver_crosschain__address_tags') }} where tag_type = 'sdt_top_holder' and blockchain = 'terra') AND event_to_label_type IS NULL THEN 'Top Holder'
           ELSE 'Smaller Wallets'
       END AS to_segment,
       event_currency,
       sum(event_amount) as volume,
       count(distinct tx_id) AS tx_count,
      count(distinct event_from) AS address_count
FROM recent_events
WHERE (from_segment = 'Foundation' or to_segment = 'Foundation')
  AND from_segment != to_segment
  AND event_currency = 'SDT'
GROUP BY metric_date, from_segment, to_segment, event_currency


UNION

SELECT date_trunc('day', block_timestamp) AS metric_date,
       CASE
           WHEN event_from_label_type = 'operator' THEN 'Operator'
           WHEN event_from IN('terra1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8pm7utl','terra1dp0taj85ruc299rkdvzp4z5pfg6z6swaed74e6') THEN 'Foundation'
           WHEN event_from_label_type IN('distributor','cex') THEN 'Exchanges'
           WHEN event_from IN(select distinct address from {{ ref('silver_crosschain__address_tags') }} where tag_type = 'luna_top_holder' and blockchain = 'terra') AND event_from_label_type IS NULL THEN 'Top Holder'
           ELSE 'Smaller Wallets'
       END AS from_segment,
       CASE
           WHEN event_to_label_type = 'operator' THEN 'Operator'
           WHEN event_to IN('terra1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8pm7utl','terra1dp0taj85ruc299rkdvzp4z5pfg6z6swaed74e6') THEN 'Foundation'
           WHEN event_to_label_type IN('distributor','cex') THEN 'Exchanges'
           WHEN event_to IN(select distinct address from {{ ref('silver_crosschain__address_tags') }} where tag_type = 'luna_top_holder' and blockchain = 'terra') AND event_to_label_type IS NULL THEN 'Top Holder'
           ELSE 'Smaller Wallets'
       END AS to_segment,
       event_currency,
       sum(event_amount) as volume,
       count(distinct tx_id) AS tx_count,
      count(distinct event_from) AS address_count
FROM recent_events
WHERE (from_segment = 'Foundation' or to_segment = 'Foundation')
  AND from_segment != to_segment
  AND event_currency = 'LUNA'
GROUP BY metric_date, from_segment, to_segment, event_currency

UNION

SELECT date_trunc('day', block_timestamp) AS metric_date,
       CASE
           WHEN event_from_label_type = 'operator' THEN 'Operator'
           WHEN event_from IN('terra1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8pm7utl','terra1dp0taj85ruc299rkdvzp4z5pfg6z6swaed74e6') THEN 'Foundation'
           WHEN event_from_label_type IN('distributor','cex') THEN 'Exchanges'
           WHEN event_from IN(select distinct address from {{ ref('silver_crosschain__address_tags') }} where tag_type = 'ust_top_holder' and blockchain = 'terra') AND event_from_label_type IS NULL THEN 'Top Holder'
           ELSE 'Smaller Wallets'
       END AS from_segment,
       CASE
           WHEN event_to_label_type = 'operator' THEN 'Operator'
           WHEN event_to IN('terra1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8pm7utl','terra1dp0taj85ruc299rkdvzp4z5pfg6z6swaed74e6') THEN 'Foundation'
           WHEN event_to_label_type IN('distributor','cex') THEN 'Exchanges'
           WHEN event_to IN(select distinct address from {{ ref('silver_crosschain__address_tags') }} where tag_type = 'ust_top_holder' and blockchain = 'terra') AND event_to_label_type IS NULL THEN 'Top Holder'
           ELSE 'Smaller Wallets'
       END AS to_segment,
       event_currency,
       sum(event_amount) as volume,
       count(distinct tx_id) AS tx_count,
      count(distinct event_from) AS address_count
FROM recent_events
WHERE (from_segment = 'Foundation' or to_segment = 'Foundation')
  AND from_segment != to_segment
  AND event_currency = 'UST'
GROUP BY metric_date, from_segment, to_segment, event_currency
  
  )
         
select 
metric_date,
from_segment,
to_segment,
event_currency as currency,
CASE
    WHEN from_segment = 'Foundation' THEN -(volume)
    WHEN to_segment = 'Foundation' THEN volume
    END AS volume,
CASE
    WHEN from_segment = 'Foundation' then tx_count
    ELSE 0
    END AS tx_outflow,
CASE
    WHEN to_segment = 'Foundation' then tx_count
    ELSE 0
    END AS tx_inflow,
CASE
    WHEN from_segment = 'Foundation' then address_count
    ELSE 0
    END AS address_outflow,
CASE
    WHEN to_segment = 'Foundation' then address_count
    ELSE 0
    END AS address_inflow
from foundation_flows
