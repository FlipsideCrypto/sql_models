{{ 
  config(
    materialized='incremental', 
    unique_key="CONCAT_WS('-', ADDRESS, ASSET_ID)", 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'algorand', 'asset_id']
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
  FROZEN as FROZEN,
  _FIVETRAN_SYNCED

  
FROM {{source('algorand','ACCOUNT_ASSET')}} aa
left join asset_name an on aa.ASSETID = an.INDEX

where
1=1 
{% if is_incremental() %}
AND _FIVETRAN_SYNCED >= (
  SELECT
    MAX(
      _FIVETRAN_SYNCED
    )
  FROM
    {{ this }} 
)
{% endif %}