{{ 
  config(
    materialized='incremental', 
    sort='CREATED_AT', 
    unique_key='ASSET_ID', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'algorand', 'asset']
  )
}}



select 
INDEX as ASSET_ID,
CREATOR_ADDR :: STRING as CREATOR_ADDRESS,
params:t :: NUMBER as TOTAL_SUPPLY,
params:an :: STRING as ASSET_NAME,
params:au :: STRING as ASSET_URL,
params:dc as DECIMALS,
DELETED as ASSET_DELETED,
CLOSED_AT as CLOSED_AT,
CREATED_AT as CREATED_AT

FROM {{source('algorand','ASSET')}}


