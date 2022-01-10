{{ config(
  materialized = 'incremental',
  unique_key = '_unique_key',
  incremental_strategy = 'merge',
  tags = ['snowflake', 'all_algorand', 'transactions', 'all_algorand_tx']
) }}

WITH emptyROUNDS AS (

  SELECT
    ROUND,
    intra,
    txn,
    _FIVETRAN_SYNCED
  FROM
    {{ source(
      'algorand',
      'TXN'
    ) }}
  WHERE
    txid :: STRING = ''
),
fulljson AS (
  SELECT
    ROUND,
    txid,
    intra,
    t.value AS message,
    txn,
    txn :txn :gh :: STRING AS gh
  FROM
    {{ source(
      'algorand',
      'TXN'
    ) }},
    LATERAL FLATTEN(
      input => txn :dt :itx
    ) t
  WHERE
    txn :dt :itx IS NOT NULL
)
SELECT
  f.round AS txn_round,
  er.round AS inner_round,
  f.txid AS txn_txn_id,
  f.txn AS txn_body,
  f.message AS txn_inner_txn,
  er.txn AS inner_message,
  er.intra AS inner_intra,
  f.intra AS txn_intra,
  f.gh AS genisis_hash,
  concat_ws(
    '-',
    er.round :: STRING,
    er.intra :: STRING
  ) AS _unique_key,
  _FIVETRAN_SYNCED
FROM
  emptyROUNDS er
  LEFT JOIN fulljson f
  ON er.txn = f.message
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
