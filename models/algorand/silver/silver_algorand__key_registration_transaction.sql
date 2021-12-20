{{ config(
  materialized = 'incremental',
  unique_key = '_unique_key',
  incremental_strategy = 'merge',
  tags = ['snowflake', 'algorand', 'key_registration', 'silver_algorand']
) }}

SELECT
  intra,
  b.round AS block_id,
  txn :txn :grp :: STRING AS tx_group_id,
  HEX_DECODE_STRING(
    txid :: text
  ) AS tx_id,
  asset AS asset_id,
  algorand_decode_b64_addr(
    txn :txn :snd :: text
  ) AS sender,
  txn :txn :fee / pow(
    10,
    6
  ) AS fee,
  algorand_decode_b64_addr(
    txn :txn :votekey :: text
  ) AS participation_key,
  algorand_decode_b64_addr(
    txn :txn :selkey :: text
  ) AS vrf_public_key,
  txn :txn :votefst AS vote_first,
  txn :txn :votelst AS vote_last,
  txn :txn :votekd AS vote_keydilution,
  csv.type AS tx_type,
  csv.name AS tx_type_name,
  txn :txn :gh :: STRING AS genisis_hash,
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
  b.typeenum = 2

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
