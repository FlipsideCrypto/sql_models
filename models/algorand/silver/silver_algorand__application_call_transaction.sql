{{ config(
  materialized = 'incremental',
  unique_key = '_unique_key',
  incremental_strategy = 'merge',
  tags = ['snowflake', 'algorand', 'application_call', 'silver_algorand_tx']
) }}

WITH allTXN AS (

  SELECT
    block_timestamp,
    b.intra,
    block_id,
    tx_group_id,
    tx_id,
    inner_tx,
    tx_message :txn :snd :: text AS sender,
    fee,
    tx_message :txn :apid AS app_id,
    tx_type,
    tx_type_name,
    genesis_hash,
    tx_message,
    extra,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver_algorand__transactions') }}
    b
  WHERE
    tx_type = 'appl'
)
SELECT
  block_timestamp,
  intra,
  b.block_id,
  tx_group_id,
  tx_id,
  TO_BOOLEAN(inner_tx) AS inner_tx,
  NULL AS asset_id,
  algorand_decode_b64_addr(
    sender
  ) AS sender,
  fee,
  app_id,
  tx_type,
  tx_type_name,
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
