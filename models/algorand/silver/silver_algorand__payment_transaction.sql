{{ config(
  materialized = 'incremental',
  unique_key = '_unique_key',
  incremental_strategy = 'merge',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'algorand', 'payment', 'silver_algorand_tx']
) }}

WITH allTXN AS (

  SELECT
    block_timestamp,
    intra,
    block_id,
    tx_group_id,
    tx_id,
    inner_tx,
    asset_id,
    tx_message :txn :snd :: text AS sender,
    tx_message :txn :rcv :: text AS receiver,
    tx_message :txn :amt / pow(
      10,
      6
    ) AS amount,
    fee,
    tx_type,
    genesis_hash,
    tx_message,
    extra,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver_algorand__transactions') }}
  WHERE
    tx_type = 'pay'
)
SELECT
  block_timestamp,
  b.intra,
  b.block_id,
  tx_group_id,
  tx_id,
  inner_tx,
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
