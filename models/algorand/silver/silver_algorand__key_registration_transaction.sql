{{ config(
  materialized = 'incremental',
  unique_key = '_unique_key',
  incremental_strategy = 'merge',
  tags = ['snowflake', 'algorand', 'key_registration', 'silver_algorand_tx']
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
    fee,
    tx_message :txn :votekey :: text AS participation_key,
    tx_message :txn :selkey :: text AS vrf_public_key,
    tx_message :txn :votefst AS vote_first,
    tx_message :txn :votelst AS vote_last,
    tx_message :txn :votekd AS vote_keydilution,
    tx_message :txn :type :: STRING AS tx_type,
    genesis_hash,
    tx_message,
    extra,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver_algorand__transactions') }}
  WHERE
    tx_type = 'keyreg'
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
    participation_key
  ) AS participation_key,
  algorand_decode_b64_addr(
    vrf_public_key
  ) AS vrf_public_key,
  vote_first,
  vote_last,
  vote_keydilution,
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
