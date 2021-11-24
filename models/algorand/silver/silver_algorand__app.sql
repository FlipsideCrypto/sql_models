{{ 
  config(
    materialized='incremental', 
    unique_key="CONCAT_WS('-', APP_ID)", 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'algorand', 'app']
  )
}}

select 
INDEX as APP_ID,
CREATOR :: STRING as CREATOR_ADDRESS,
DELETED as APP_CLOSED,
CLOSED_AT as CLOSED_AT,
CREATED_AT as CREATED_AT,
PARAMS,
_FIVETRAN_SYNCED


FROM {{source('algorand','APP')}}

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