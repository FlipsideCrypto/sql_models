{{ 
  config(
    materialized='incremental', 
    sort='CREATED_AT', 
    unique_key='ADDRESS', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'algorand', 'account']
  )
}}

SELECT
  ADDR :: STRING as ADDRESS,
  DELETED as ACCOUNT_CLOSED,
  REWARDSBASE * POW(10,6) as REWARDSBASE,
  MICROALGOS * POW(10,6)  as BALANCE,
  CLOSED_AT as CLOSED_AT,
  CREATED_AT as CREATED_AT,
  KEYTYPE as WALLET_TYPE,
  ACCOUNT_DATA as ACCOUNT_DATA

FROM {{source('algorand','ACCOUNT')}}

