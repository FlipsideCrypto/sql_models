{{ 
  config(
    materialized='incremental', 
    sort='BLOCK_ID', 
    unique_key='BLOCK_ID || INTRA || ADDRRESS', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'algorand', 'block']
  )
}}

Select 
ROUND as BLOCK_ID,
INTRA,
ADDR :: STRING as ADDRESS
FROM {{source('algorand','TXN_PARTICIPATION')}}