{{ config(
  materialized = 'incremental',
  unique_key = '_unique_key',
  incremental_strategy = 'merge',
  tags = ['snowflake', 'algorand', 'asset_freeze', 'silver_algorand_tx']
) }}

WITH allTXN_fivetran AS (

  SELECT
    ab.block_timestamp AS block_timestamp,
    b.intra,
    b.round AS block_id,
    txn :txn :grp :: STRING AS tx_group_id,
    CASE
      WHEN b.txid IS NULL THEN ft.txn_txn_id :: text
      ELSE b.txid :: text
    END AS tx_id,
    CASE
      WHEN b.txid IS NULL THEN 'true'
      ELSE 'false'
    END AS inner_tx,
    asset AS asset_id,
    txn :txn :fadd :: text AS asset_address,
    txn :txn :afrz AS asset_freeze,
    txn :txn :snd :: text AS sender,
    txn :txn :fee / pow(
      10,
      6
    ) AS fee,
    txn :txn :type :: STRING AS tx_type,
    CASE
      WHEN b.txid IS NULL THEN ft.genesis_hash :: text
      ELSE txn :txn :gh :: STRING
    END AS genesis_hash,
    txn AS tx_message,
    extra
  FROM
    {{ source(
      'algorand',
      'TXN'
    ) }}
    b
    LEFT JOIN {{ ref('silver_algorand__inner_txids') }}
    ft
    ON b.round = ft.inner_round
    AND b.intra = ft.inner_intra
    LEFT JOIN {{ ref('silver_algorand__block') }}
    ab
    ON b.round = ab.block_id
  WHERE
    tx_type = 'afrz'

{% if is_incremental() %}
AND b.__HEVO__LOADED_AT >= (
  SELECT
    MAX(
      _INSERTED_TIMESTAMP
    )
  FROM
    {{ this }}
)
{% endif %}
),
allTXN_hevo AS (
  SELECT
    ab.block_timestamp AS block_timestamp,
    b.intra AS intra,
    b.round AS block_id,
    txn :txn :grp :: STRING AS tx_group_id,
    CASE
      WHEN b.txid IS NULL THEN ft.txn_txn_id :: text
      ELSE b.txid :: text
    END AS tx_id,
    CASE
      WHEN b.txid IS NULL THEN 'true'
      ELSE 'false'
    END AS inner_tx,
    asset AS asset_id,
    txn :txn :fadd :: text AS asset_address,
    txn :txn :afrz AS asset_freeze,
    txn :txn :snd :: text AS sender,
    txn :txn :fee / pow(
      10,
      6
    ) AS fee,
    txn :txn :type :: STRING AS tx_type,
    CASE
      WHEN b.txid IS NULL THEN ft.genesis_hash :: text
      ELSE txn :txn :gh :: STRING
    END AS genesis_hash,
    txn AS tx_message,
    extra
  FROM
    {{ source(
      'algorand',
      'TXN_MISSING'
    ) }}
    b
    LEFT JOIN {{ ref('silver_algorand__inner_txids') }}
    ft
    ON b.round = ft.inner_round
    AND b.intra = ft.inner_intra
    LEFT JOIN {{ ref('silver_algorand__block') }}
    ab
    ON b.round = ab.block_id
  WHERE
    tx_type = 'afrz'
    AND b.round > (
      SELECT
        MAX(ROUND)
      FROM
        {{ source(
          'algorand',
          'TXN'
        ) }}
    )

{% if is_incremental() %}
AND b.__HEVO__LOADED_AT >= (
  SELECT
    MAX(
      _INSERTED_TIMESTAMP
    )
  FROM
    {{ this }}
)
{% endif %}
),
allTXN AS(
  SELECT
    *
  FROM
    allTXN_fivetran
  UNION
  SELECT
    *
  FROM
    allTXN_hevo
)
SELECT
  block_timestamp,
  intra,
  block_id,
  tx_group_id,
  HEX_DECODE_STRING(
    tx_id
  ) AS tx_id,
  TO_BOOLEAN(inner_tx) AS inner_tx,
  asset_id,
  algorand_decode_b64_addr(
    asset_address
  ) AS asset_address,
  asset_freeze,
  algorand_decode_b64_addr(
    sender
  ) AS sender,
  fee,
  csv.type AS tx_type,
  csv.name AS tx_type_name,
  genesis_hash,
  tx_message,
  extra,
  concat_ws(
    '-',
    block_id :: STRING,
    intra :: STRING
  ) AS _unique_key,
  SYSDATE() AS _inserted_timestamp
FROM
  allTXN b
  LEFT JOIN {{ ref('silver_algorand__transaction_types') }}
  csv
  ON b.tx_type = csv.type
