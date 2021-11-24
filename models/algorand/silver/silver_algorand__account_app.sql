{{ 
  config(
    materialized='incremental', 
    unique_key='unique_key', 
    incremental_strategy='merge',
    tags=['snowflake', 'algorand', 'account_app']
  )
}}


SELECT
  ADDR :: STRING as ADDRESS,
  APP as APP_ID,
  DELETED as APP_CLOSED,
  CLOSED_AT as CLOSED_AT,
  CREATED_AT as CREATED_AT,
  LOCALSTATE as APP_INFO,
  CONCAT_WS('-', ADDR:: STRING , APP :: STRING) as unique_key,
  _FIVETRAN_SYNCED


FROM {{source('algorand','ACCOUNT_APP')}} 



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