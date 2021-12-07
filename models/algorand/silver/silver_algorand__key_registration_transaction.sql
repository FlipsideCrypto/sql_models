{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', BLOCK_ID, INTRA)",
  incremental_strategy = 'merge',
  tags = ['snowflake', 'algorand', 'key_registration']
) }}

SELECT
  intra,
  b.round AS block_id,
  txn :txn :grp :: STRING AS tx_group_id,
  txid :: STRING AS tx_id,
  asset AS asset_id,
  txn :txn :snd :: STRING AS sender,
  txn :txn :fee * pow(
    10,
    6
  ) AS fee,
  txn :txn :votekey :: STRING AS participation_key,
  txn :txn :selkey :: STRING AS vrf_public_key,
  txn :txn :votefst AS vote_first,
  txn :txn :votelst AS vote_last,
  txn :txn :votekd AS vote_keydilution,
  csv.type AS tx_type,
  csv.name AS tx_type_name,
  txn :txn :gh :: STRING AS genisis_hash,
  txn AS tx_message,
  extra,
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
