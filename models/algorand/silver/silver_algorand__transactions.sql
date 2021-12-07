{{ config(
  materialized = 'incremental',
  unique_key = '_unique_key',
  incremental_strategy = 'merge',
  tags = ['snowflake', 'algorand', 'transactions']
) }}

SELECT
  intra,
  b.round AS block_id,
  txn :TXN :grp :: STRING AS tx_group_id,
  txid :: STRING AS tx_id,
  asset AS asset_id,
  txn :txn :snd :: STRING AS sender,
  txn :txn :fee * pow(
    10,
    6
  ) AS fee,
  csv.type AS tx_type,
  csv.name AS tx_type_name,
  txn :TXN :GH :: STRING AS genisis_hash,
  txn AS tx_message,
  extra,
  concat_ws(
    '-',
    block_id :: STRING,
    intra :: STRING
  ) AS _unique_key,
  _FIVETRAN_SYNCED
FROM
  {{ source(
    'algorand',
    'TXN'
  ) }}
  b
  LEFT JOIN {{ ref('silver_algorand__transaction_types') }}
  csv
  ON b.typeenum = csv.typeenum
WHERE
  1 = 1

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
