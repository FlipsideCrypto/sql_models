{{ 
  config(
    materialized='incremental', 
    sort='CREATED_AT', 
    unique_key='ADDRESS || ASSET_ID', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'algorand', 'account_asset']
  )
}}

WITH asset_name as (
SELECT distinct(INDEX) as INDEX, 
params:an::STRING as name
FROM {{source('algorand','ASSET')}}
)

SELECT
  ADDR :: STRING as ADDRESS,
  ASSETID as ASSET_ID,
  an.name :: STRING as ASSET_NAME,
  AMOUNT as AMOUNT,
  CREATED_AT as ASSET_ADDED_AT,
  CLOSED_AT as ASSET_LAST_REMOVED,
  DELETED as ASSET_CLOSED,
  FROZEN as FROZEN
FROM {{source('algorand','ACCOUNT_ASSET')}} aa
left join asset_name an on aa.ASSETID = an.INDEX

