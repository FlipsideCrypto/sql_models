{{ config(
  materialized = 'incremental',
  unique_key = '_unique_key',
  incremental_strategy = 'merge',
  tags = ['snowflake', 'algorand', 'transactions', 'all_algorand_tx']
) }}

WITH emptyROUNDS AS (

  SELECT
    ROUND,
    intra,
    txn,
    extra,
    _FIVETRAN_SYNCED
  FROM
    {{ source(
      'algorand',
      'TXN'
    ) }}
  WHERE
    txid IS NULL
),
fulljson AS (
  SELECT
    ROUND,
    txid,
    intra,
    txn :txn :gh :: STRING AS gh
  FROM
    {{ source(
      'algorand',
      'TXN'
    ) }}
  WHERE
    txid IS NOT NULL
)
SELECT
  f.round AS txn_round,
  er.round AS inner_round,
  f.txid AS txn_txn_id,
  er.intra AS inner_intra,
  f.intra AS txn_intra,
  f.gh AS genesis_hash,
  concat_ws(
    '-',
    er.round :: STRING,
    er.intra :: STRING
  ) AS _unique_key,
  _FIVETRAN_SYNCED
FROM
  emptyROUNDS er
  LEFT JOIN fulljson f
  ON er.extra :"root-intra" :: NUMBER = f.intra
  AND er.round = f.round
WHERE
  f.round IS NOT NULL

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
