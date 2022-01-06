{{ config(
  materialized = 'incremental',
  unique_key = '_unique_key',
  incremental_strategy = 'merge',
  tags = ['snowflake', 'algorand', 'payment', 'silver_algorand_tx']
) }}

WITH innerRefTX AS (

  SELECT
    intra,
    ROUND,
    txn :txn :gh :: STRING AS gh,
    CASE
      WHEN txid :: STRING = '' THEN NULL
      ELSE txid
    END AS txid
  FROM
    {{ source(
      'algorand',
      'TXN'
    ) }}
),
filledTX AS(
  SELECT
    COALESCE(gh, LAST_VALUE(gh ignore nulls) over (PARTITION BY ROUND
  ORDER BY
    intra rows BETWEEN unbounded preceding
    AND CURRENT ROW)) AS filled_gh,
    COALESCE(txid, LAST_VALUE(txid ignore nulls) over (PARTITION BY ROUND
  ORDER BY
    intra rows BETWEEN unbounded preceding
    AND CURRENT ROW)) AS filled_tx,*
  FROM
    innerRefTX
  ORDER BY
    ROUND ASC,
    intra ASC
),
allTXN AS (
  SELECT
    b.intra,
    b.round AS block_id,
    txn :txn :grp :: STRING AS tx_group_id,
    CASE
      WHEN b.txid :: STRING = '' THEN ft.filled_tx :: text
      ELSE b.txid :: text
    END AS tx_id,
    CASE
      WHEN b.txid :: STRING = '' THEN 'true'
      ELSE 'false'
    END AS inner_tx,
    asset AS asset_id,
    txn :txn :snd :: text AS sender,
    txn :txn :rcv :: text AS receiver,
    txn :txn :amt / pow(
      10,
      6
    ) AS amount,
    txn :txn :fee / pow(
      10,
      6
    ) AS fee,
    txn :txn :type :: STRING AS tx_type,
    CASE
      WHEN b.txid :: STRING = '' THEN ft.filled_gh :: text
      ELSE txn :txn :gh :: STRING
    END AS genisis_hash,
    txn AS tx_message,
    extra,
    _FIVETRAN_SYNCED
  FROM
    {{ source(
      'algorand',
      'TXN'
    ) }}
    b
    LEFT JOIN filledTX ft
    ON b.round = ft.round
    AND b.intra = ft.intra
  WHERE
    tx_type = 'pay'
)
SELECT
  intra,
  block_id,
  tx_group_id,
  HEX_DECODE_STRING(
    tx_id
  ) AS tx_id,
  TO_BOOLEAN(inner_tx) AS inner_tx,
  asset_id,
  algorand_decode_b64_addr(
    sender
  ) AS sender,
  algorand_decode_b64_addr(
    receiver
  ) AS receiver,
  amount,
  fee,
  csv.type AS tx_type,
  csv.name AS tx_type_name,
  genisis_hash,
  tx_message,
  extra,
  concat_ws(
    '-',
    block_id :: STRING,
    intra :: STRING
  ) AS _unique_key,
  _FIVETRAN_SYNCED
FROM
  allTXN b
  LEFT JOIN {{ ref('silver_algorand__transaction_types') }}
  csv
  ON b.tx_type = csv.type
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
