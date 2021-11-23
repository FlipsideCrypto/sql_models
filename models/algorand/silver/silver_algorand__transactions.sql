{{ 
  config(
    materialized='incremental', 
    sort='BLOCK_ID', 
    unique_key='BLOCK_ID || INTRA', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'algorand', 'block']
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
EXTRA



FROM {{source('algorand','TXN')}} b
left join {{ref('silver_algorand__transaction_types')}} csv on b.TYPEENUM = csv.TYPEENUM



