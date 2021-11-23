{{ 
  config(
    materialized='incremental', 
    sort='CREATED_AT', 
    unique_key='ADDRESS || APP_ID', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'algorand', 'account_app']
  )
}}


SELECT
  ADDR :: STRING as ADDRESS,
  APP as APP_ID,
  DELETED as APP_CLOSED,
  CLOSED_AT as CLOSED_AT,
  CREATED_AT as CREATED_AT,
  LOCALSTATE as APP_INFO


FROM {{source('algorand','ACCOUNT_APP')}}

