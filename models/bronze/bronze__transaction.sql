{{ config (
    materialized = 'view'
) }}

SELECT
    ROUND,
    intra,
    typeenum,
    asset,
    txid,
    txnbytes,
    txn,
    extra,
    __HEVO__LOADED_AT,
    DATEADD(
        ms,
        __HEVO__LOADED_AT,
        '1970-01-01'
    ) AS _inserted_timestamp
FROM
    {{ source(
        'algorand',
        'TXN'
    ) }}
