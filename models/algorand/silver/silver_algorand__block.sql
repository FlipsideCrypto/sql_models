{{ 
  config(
    materialized='incremental', 
    sort='BLOCK_ID', 
    unique_key='BLOCK_ID', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'algorand', 'block']
  )
}}


select
ROUND as BLOCK_ID,
REALTIME :: TIMESTAMP as BLOCK_TIMESTAMP,
REWARDSLEVEL as REWARDSLEVEL,
HEADER:gen :: STRING as NETWORK,
HEADER:gh :: STRING as GENISIS_HASH,
HEADER:prev :: STRING as PREV_BLOCK_HASH,
HEADER:txn :: STRING as TXN_ROOT,
HEADER

FROM {{source('algorand','BLOCK_HEADER')}}
