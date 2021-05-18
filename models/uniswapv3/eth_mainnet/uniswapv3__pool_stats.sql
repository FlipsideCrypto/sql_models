{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp',
    unique_key='block_id', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'uniswapv3', 'pool_stats']
  )
}}


{{ 
  uniswapv3_pool_stats(
    source('uniswapv3_eth', 'uniswapv3_pool_stats'), 
    source('uniswapv3_eth', 'uniswapv3_pools'),
    ref("ethereum__token_prices_hourly_v2")
  ) 
}}
