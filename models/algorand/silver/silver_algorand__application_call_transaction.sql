{{ 
  config(
    materialized='incremental', 
    unique_key="CONCAT_WS('-', BLOCK_ID, INTRA)", 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'algorand', 'application_call']
  )
}}

select 
INTRA,
ROUND as BLOCK_ID,
txn:txn:grp :: STRING as TX_GROUP_ID,
TXID :: STRING as TX_ID,
ASSET as ASSET_ID,
txn:txn:snd :: STRING as SENDER,
txn:txn:fee * POW(10,6) as FEE,
txn:txn:apid as APP_ID,
csv.type as TX_TYPE,
csv.name as TX_TYPE_NAME,
txn:txn:gh :: STRING as GENISIS_HASH,
txn as TX_MESSAGE,
EXTRA,
_FIVETRAN_SYNCED


FROM {{source('algorand','TXN')}} b
left join {{ref('silver_algorand__transaction_types')}} csv on b.TYPEENUM = csv.TYPEENUM
where b.TYPEENUM = 6

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