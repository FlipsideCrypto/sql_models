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
txn:txn:grp :: STRING as TX_GROUP_ID,
TXID :: STRING as TX_ID,
ASSET as ASSET_ID,
txn:txn:snd :: STRING as SENDER,
txn:txn:fee * POW(10,6) as FEE,
txn:txn:votekey :: STRING as PARTICIPATION_KEY,
txn:txn:selkey :: STRING as VRF_PUBLIC_KEY,
txn:txn:votefst as VOTE_FIRST,
txn:txn:votelst as VOTE_LAST,
txn:txn:votekd as VOTE_KEYDILUTION,
csv.type as TX_TYPE,
csv.name as TX_TYPE_NAME,
txn:txn:gh :: STRING as GENISIS_HASH,
TXN as TX_MESSAGE,
EXTRA



FROM {{source('algorand','TXN')}} b
left join {{ref('silver_algorand__transaction_types')}} csv on b.TYPEENUM = csv.TYPEENUM
where b.TYPEENUM = 2