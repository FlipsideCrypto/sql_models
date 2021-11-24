{{ 
  config(
    materialized='incremental', 
    unique_key='unique_key', 
    incremental_strategy='merge',
    tags=['snowflake', 'algorand', 'transactions']
  )
}}

select 
INTRA,
ROUND as BLOCK_ID,
TXN:TXN:grp :: STRING as TX_GROUP_ID,
TXID :: STRING as TX_ID,
ASSET as ASSET_ID,
txn:txn:snd :: STRING as SENDER,
txn:txn:fee * POW(10,6) as FEE,
csv.type as TX_TYPE,
csv.name as TX_TYPE_NAME,
TXN:TXN:GH :: STRING as GENISIS_HASH,
TXN as TX_MESSAGE,
EXTRA,
CONCAT_WS('-', BLOCK_ID :: STRING, INTRA :: STRING) as unique_key,
_FIVETRAN_SYNCED


FROM {{source('algorand','TXN')}} b
left join {{ref('silver_algorand__transaction_types')}} csv on b.TYPEENUM = csv.TYPEENUM

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
