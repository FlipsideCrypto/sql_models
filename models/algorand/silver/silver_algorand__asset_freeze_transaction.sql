{{ config(
  materialized = 'incremental',
  unique_key = '_unique_key',
  incremental_strategy = 'merge',
  tags = ['snowflake', 'algorand', 'asset_freeze', 'silver_algorand_tx']
) }}

WITH allTXN AS (

  SELECT
    block_timestamp,
    b.intra,
    block_id,
    tx_group_id,
    tx_id,
    inner_tx,
    asset_id,
    tx_message :txn :fadd :: text AS asset_address,
    tx_message :txn :afrz AS asset_freeze,
    tx_message :txn :snd :: text AS sender,
    fee,
    tx_type,
    genesis_hash,
    tx_message,
    extra,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver_algorand__transactions') }}
    b
  WHERE
    tx_type = 'afrz'
)
SELECT
  block_timestamp,
  intra,
  block_id,
  tx_group_id,
  tx_id,
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
  b._INSERTED_TIMESTAMP
FROM
  allTXN b
  LEFT JOIN {{ ref('silver_algorand__transaction_types') }}
  csv
  ON b.tx_type = csv.type
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
