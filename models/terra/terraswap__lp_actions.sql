{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='block_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp'],
    tags=['snowflake', 'terra', 'terraswap', 'lp']
  )
}}

with msgs as (
SELECT 
  m.block_id,
  m.block_timestamp,
  m.tx_id,
  msg_value:sender::string as sender,
  'provide_liquidity' as action,
  msg_value:execute_msg:provide_liquidity:assets[0]:amount / POW(10,6) as token_0_amount,
  coalesce(msg_value:execute_msg:provide_liquidity:assets[0]:info:token:contract_addr::string,
            msg_value:execute_msg:provide_liquidity:assets[0]:info:native_token:denom::string) as token_0_address,
  msg_value:execute_msg:provide_liquidity:assets[1]:amount / POW(10,6) as token_1_amount,
  coalesce(msg_value:execute_msg:provide_liquidity:assets[1]:info:token:contract_addr::string,
            msg_value:execute_msg:provide_liquidity:assets[1]:info:native_token:denom::string) as token_1_address,
  msg_value:contract::string as pool_address
FROM {{source('silver_terra', 'msgs')}} m

WHERE msg_value:execute_msg:provide_liquidity IS NOT NULL --Ensures we only look for adding liquidity events
),

events as (
SELECT 
  tx_id,
  event_attributes:share/POW(10,6) as lp_share_amount
  event_attributes:"2_contract_address"::string as lp_pool_address
FROM {{source('silver_terra', 'msgs')}}

WHERE tx_id IN(SELECT DISTINCT tx_id 
                FROM msgs)
  AND event_type = 'from_contract'
  AND event_attributes:"0_action"::string = 'provide_liquidity'

)