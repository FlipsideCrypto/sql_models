{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp',
    unique_key='tx_id', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'uniswapv3', 'swaps']
  )
}}


{{ 
  uniswapv3_swaps(
	  source('uniswapv3_eth', 'uniswapv3_swaps'),
    source('uniswapv3_eth', 'uniswapv3_pools'),
    ref("ethereum__token_prices_hourly_v2")
  )
}}
