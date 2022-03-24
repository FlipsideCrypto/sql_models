{{ config(
  materialized = 'incremental',
  unique_key = '_unique_key',
  incremental_strategy = 'merge',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'algorand', 'asset_transfer', 'silver_algorand_tx']
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
    tx_message :txn :snd :: text AS sender,
    fee,
    tx_message :txn :asnd :: text AS asset_sender,
    tx_message :txn :arcv :: text AS asset_receiver,
    tx_message :txn :aamt AS asset_amount,
    tx_message :txn :xaid AS asset_transferred,
    tx_type,
    genesis_hash,
    tx_message,
    extra,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver_algorand__transactions') }}
    b
  WHERE
    tx_type = 'axfer'
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
    sender
  ) AS sender,
  fee,
  algorand_decode_b64_addr(
    asset_sender
  ) AS asset_sender,
  algorand_decode_b64_addr(
    asset_receiver
  ) AS asset_receiver,
  asset_amount,
  asset_transferred,
  csv.type AS tx_type,
  csv.name AS tx_type_name,
  genesis_hash,
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
) - INTERVAL '4 HOURS'
{% endif %}
