{{ 
  config(
    materialized='incremental', 
    unique_key="CONCAT_WS('-', ASSET_ID)", 
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
CREATED_AT as CREATED_AT,
_FIVETRAN_SYNCED

FROM {{source('algorand','ASSET')}}

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

