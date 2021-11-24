{{ 
  config(
    materialized='incremental', 
    unique_key="CONCAT_WS('-', BLOCK_ID, INTRA, ADDRESS)", 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'algorand', 'transaction_participation']
  )
}}

Select 
ROUND as BLOCK_ID,
INTRA,
ADDR :: STRING as ADDRESS,
_FIVETRAN_SYNCED

FROM {{source('algorand','TXN_PARTICIPATION')}}

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