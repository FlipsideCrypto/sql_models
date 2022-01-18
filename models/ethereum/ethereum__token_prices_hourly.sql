{{ 
  config(
    materialized='incremental', 
    sort='hour', 
    unique_key='hour', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'ethereum', 'events']
  )
}}

-- SELECT
--   p.symbol,
--   date_trunc('hour', recorded_at) as hour,
--   lower(a.token_address) as token_address,
--   d.decimals,
--   avg(price) as price
-- FROM
--   {{ source('shared', 'prices_v2')}} p
-- JOIN
--   {{ source('shared', 'market_asset_metadata')}} a
-- ON
--   p.asset_id = a.asset_id
-- LEFT OUTER JOIN
--   {{ source('ethereum', 'ethereum_contract_decimal_adjustments')}} d
-- ON
--   d.address = lower(a.token_address)
-- WHERE
--     (a.platform_id = '1027' OR a.asset_id = '1027' or a.platform_id = 'ethereum')
--    {% if is_incremental() %}
--      AND recorded_at >= getdate() - interval '45 days'
--    {% else %}
--      AND recorded_at >= '2020-05-05'::timestamp
--    {% endif %}

-- GROUP BY 1,2,3,4

with hours as (
    select dateadd(hour, -seq4(), date_trunc('hour',current_timestamp())) as day_hour
    from 
    -- get all hours since some date
    {% if is_incremental() %}
        table(generator(rowcount=>50*24))
    {% else %}
        table(generator(rowcount=>(select max(days_since) * 24 from {{ ref('ethereum_dbt__days_since_for_prices_hourly') }})))
    {% endif %}              
),
hourly_prices as (
    select
      p.symbol,
      date_trunc('hour', recorded_at) as hour,
      lower(a.token_address) as token_address,
      d.decimals,
      avg(price) as price
    from {{ source('shared', 'prices_v2') }} p
    left outer join {{ source('shared', 'market_asset_metadata') }} a on p.asset_id = a.asset_id
    left outer join {{ source('ethereum', 'ethereum_contract_decimal_adjustments') }} d on d.address = lower(a.token_address)
    where (a.platform_id = '1027' or a.asset_id = '1027' or a.platform_id = 'ethereum')
    {% if is_incremental() %}
        and recorded_at >= current_date - 45
    {% else %}
        and recorded_at >= '2018-02-21' -- first date with valid prices data for a symbol
    {% endif %}  
    group by 1,2,3,4
)
, symbols as (
    select distinct symbol, token_address
    from {{ this }}
)
, hour_symbols_pair as (
    select *
    from hours
    cross join symbols
    {% if is_incremental() %}
      where day_hour >= current_date - 45
    {% else %}
      where day_hour >= '2018-02-21' -- first date with valid prices data for a symbol
    {% endif %}  
)
, imputed as (
    select 
        h.day_hour
        , h.symbol
        , p.price as avg_price
        , p.token_address
        , p.decimals
        , last_value(p.token_address) ignore nulls over (partition by h.symbol, h.token_address order by day_hour) as lag_token_address
        , last_value(p.decimals) ignore nulls over (partition by h.symbol, h.token_address order by day_hour) as lag_decimals
        , lag(p.price) ignore nulls over (partition by h.symbol, h.token_address order by day_hour) as imputed_price
    from hour_symbols_pair h 
    left outer join hourly_prices p on p.hour = h.day_hour and p.symbol = h.symbol and p.token_address = h.token_address
)
select 
    p.day_hour as hour
    , p.symbol
    , case when token_address is not null then token_address
      else lag_token_address end as token_address
    , case when decimals is not null then decimals
      else lag_decimals end as decimals
    , case when avg_price is not null then avg_price
      else imputed_price end as price
    , case when avg_price is null then TRUE 
      else FALSE end as is_imputed
from imputed p
where price is not null


