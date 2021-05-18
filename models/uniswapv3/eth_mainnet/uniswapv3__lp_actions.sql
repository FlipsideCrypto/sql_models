{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp',
    unique_key='tx_id', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'uniswapv3', 'lp_actions']
  )
}}


{{ 
  uniswapv3_lp_actions(
    source('uniswapv3_eth', 'uniswapv3_liquidity_actions'), 
    source('uniswapv3_eth', 'uniswapv3_pools'),
    ref("ethereum__token_prices_hourly_v2")
  ) 
}}
