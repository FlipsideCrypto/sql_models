{{ 
  config(
    materialized='incremental', 
    unique_key="CONCAT_WS('-', BLOCK_ID)", 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'algorand', 'block']
  )
}}


select
ROUND as BLOCK_ID,
REALTIME :: TIMESTAMP as BLOCK_TIMESTAMP,
REWARDSLEVEL as REWARDSLEVEL,
HEADER:gen :: STRING as NETWORK,
HEADER:gh :: STRING as GENISIS_HASH,
HEADER:prev :: STRING as PREV_BLOCK_HASH,
HEADER:txn :: STRING as TXN_ROOT,
HEADER,
_FIVETRAN_SYNCED

FROM {{source('algorand','BLOCK_HEADER')}}

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
