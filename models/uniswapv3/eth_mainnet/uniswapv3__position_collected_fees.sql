{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = 'tx_id',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'uniswapv3', 'position_collected_fees']
) }}
{{ uniswapv3_position_collected_fees(ref('silver_uniswapv3__pools'), ref('ethereum__events_emitted'), ref('silver_uniswapv3__positions'), ref("ethereum__token_prices_hourly"), ref("ethereum__transactions")) }}
