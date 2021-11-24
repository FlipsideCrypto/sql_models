{{ 
  config(
    materialized='incremental',  
    unique_key="CONCAT_WS('-', ADDRESS)", 
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
  ACCOUNT_DATA as ACCOUNT_DATA,
  _FIVETRAN_SYNCED

FROM {{source('algorand','ACCOUNT')}}

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