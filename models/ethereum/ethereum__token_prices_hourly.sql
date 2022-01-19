{{ 
  config(
    materialized='incremental', 
    sort='hour', 
    unique_key='hour', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'ethereum', 'events']
  )
}}

with hourly_prices as (
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
        and recorded_at >= '2020-05-05' -- first date with valid prices data
    {% endif %}  
    group by 1,2,3,4
)
, symbols as (
    select distinct p.symbol, lower(a.token_address) as token_address
    from {{ source('shared', 'prices_v2') }} p
    left outer join {{ source('shared', 'market_asset_metadata') }} a on p.asset_id = a.asset_id 
    where (a.platform_id = '1027' or a.asset_id = '1027' or a.platform_id = 'ethereum')
)
, hour_symbols_pair as (
    select *
    from silver.hours
    cross join symbols
    {% if is_incremental() %}
      where hour >= current_date - 45
    {% else %}
      where hour >= '2020-05-05' -- first date with valid prices data
    {% endif %}  
)
, imputed as (
    select 
        h.hour
        , h.symbol
        , h.token_address
        , p.decimals
        , p.price as avg_price
        -- , last_value(p.token_address) ignore nulls over (partition by h.symbol, h.token_address order by h.hour) as lag_token_address
        , lag(p.decimals) ignore nulls over (partition by h.symbol, h.token_address order by h.hour) as lag_decimals
        , lag(p.price) ignore nulls over (partition by h.symbol, h.token_address order by h.hour) as imputed_price
    from hour_symbols_pair h 
    left outer join hourly_prices p on p.hour = h.hour and p.symbol = h.symbol and p.token_address = h.token_address
)
select 
    p.hour as hour
    , p.symbol
    , p.token_address
    , case when decimals is not null then decimals
      else lag_decimals end as decimals
    , case when avg_price is not null then avg_price
      else imputed_price end as price
    , case when avg_price is null then TRUE 
      else FALSE end as is_imputed
from imputed p
where price is not null


