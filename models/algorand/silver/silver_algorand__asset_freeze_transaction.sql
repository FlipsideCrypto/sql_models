{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', BLOCK_ID, INTRA)",
  incremental_strategy = 'merge',
  tags = ['snowflake', 'algorand', 'asset_freeze']
) }}

SELECT
  intra,
  b.round AS block_id,
  txn :txn :grp :: STRING AS tx_group_id,
  txid :: STRING AS tx_id,
  asset AS asset_id,
  txn :txn :fadd :: STRING AS asset_address,
  txn :txn :afrz AS asset_freeze,
  txn :txn :snd :: STRING AS sender,
  txn :txn :fee * pow(
    10,
    6
  ) AS fee,
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
  b.typeenum = 5

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
