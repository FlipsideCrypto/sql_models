{{ 
  config(
    materialized='incremental', 
    sort='CREATED_AT', 
    unique_key='APP_ID', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'algorand', 'account']
  )
}}

select 
INDEX as APP_ID,
CREATOR :: STRING as CREATOR_ADDRESS,
DELETED as APP_CLOSED,
CLOSED_AT as CLOSED_AT,
CREATED_AT as CREATED_AT,
PARAMS
FROM {{source('algorand','APP')}}
