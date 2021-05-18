{{ 
  config(
    materialized='incremental', 
	sort='block_timestamp',
    unique_key='tx_id', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'uniswapv3', 'position_collected_fees']
  )
}}

{{ 
  uniswapv3_position_collected_fees(
    source('uniswapv3_eth', 'uniswapv3_pools'),
    source('ethereum', 'ethereum_events_emitted'),
    source('uniswapv3_eth', 'uniswapv3_positions'),
    ref("ethereum__token_prices_hourly_v2")
  )
}}
