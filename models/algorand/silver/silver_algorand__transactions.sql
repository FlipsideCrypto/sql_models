{{ config(
  materialized = 'incremental',
  unique_key = '_unique_key',
  incremental_strategy = 'merge',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'algorand', 'transactions', 'silver_algorand_tx']
) }}

WITH allTXN AS (

  SELECT
    b.intra,
    b.round AS block_id,
    txn :txn :grp :: STRING AS tx_group_id,
    b.txid :: text AS tx_id,
    'false' AS inner_tx,
    CASE
      WHEN txn :txn :type :: STRING = 'appl' THEN NULL
      WHEN txn :txn :type :: STRING = 'pay' THEN 0
      ELSE asset
    END AS asset_id,
    txn :txn :snd :: text AS sender,
    txn :txn :fee / pow(
      10,
      6
    ) AS fee,
    txn :txn :type :: STRING AS tx_type,
    txn :txn :gh :: STRING AS genesis_hash,
    txn AS tx_message,
    extra,
    __HEVO__LOADED_AT AS _INSERTED_TIMESTAMP
  FROM
    {{ source(
      'algorand',
      'TXN'
    ) }}
    b
  WHERE
    txid IS NOT NULL
    AND ROUND > 18993228
),
innertx AS (
  SELECT
    b.intra + INDEX + 1,
    b.round AS block_id,
    txn :txn :grp :: STRING AS tx_group_id,
    b.txid :: text AS tx_id,
    'TRUE' AS inner_tx,
    CASE
      WHEN VALUE :txn :type :: STRING = 'appl' THEN NULL
      WHEN VALUE :txn :type :: STRING = 'pay' THEN 0
      ELSE VALUE :txn :xaid :: STRING
    END AS asset_id,
    VALUE :txn :snd :: text AS sender,
    CASE
      WHEN VALUE :txn :fee IS NULL THEN 0
      ELSE VALUE :txn :fee / pow(
        10,
        6
      )
    END AS fee,
    VALUE :txn :type :: STRING AS tx_type,
    txn :txn :gh :: STRING AS genesis_hash,
    VALUE AS tx_message,
    extra,
    __HEVO__LOADED_AT AS _INSERTED_TIMESTAMP
  FROM
    {{ source(
      'algorand',
      'TXN'
    ) }}
    b,
    LATERAL FLATTEN(
      input => txn :dt :itx
    ) f
  WHERE
    txn :dt :itx IS NOT NULL
    AND txid IS NOT NULL
    AND ROUND > 18993228
),
uniontxn AS(
  SELECT
    *
  FROM
    allTXN
  UNION
  SELECT
    *
  FROM
    innertx
)
SELECT
  ab.block_timestamp AS block_timestamp,
  b.intra,
  b.block_id,
  tx_group_id,
  HEX_DECODE_STRING(
    tx_id
  ) AS tx_id,
  TO_BOOLEAN(inner_tx) AS inner_tx,
  asset_id,
  algorand_decode_b64_addr(
    sender
  ) AS sender,
  fee,
  csv.type AS tx_type,
  csv.name AS tx_type_name,
  b.genesis_hash,
  tx_message,
  extra,
  concat_ws(
    '-',
    b.block_id :: STRING,
    b.intra :: STRING
  ) AS _unique_key,
  b._INSERTED_TIMESTAMP
FROM
  uniontxn b
  LEFT JOIN {{ ref('silver_algorand__transaction_types') }}
  csv
  ON b.tx_type = csv.type
  LEFT JOIN {{ ref('silver_algorand__block') }}
  ab
  ON b.block_id = ab.block_id
WHERE
  1 = 1

{% if is_incremental() %}
AND b._INSERTED_TIMESTAMP >= (
  SELECT
    MAX(
      _INSERTED_TIMESTAMP
    )
  FROM
    {{ this }}
)
OR block_timestamp IS NULL
{% endif %}
