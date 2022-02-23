{{ config(
  materialized = 'incremental',
  unique_key = '_unique_key',
  incremental_strategy = 'merge',
  tags = ['snowflake', 'algorand', 'transactions', 'all_algorand_tx']
) }}

WITH emptyROUNDS_fivetran AS (

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
emptyROUNDS_hevo AS (
  SELECT
    ROUND,
    intra,
    txn,
    extra,
    DATEADD(
      'MS',
      __HEVO__LOADED_AT,
      '1970-01-01'
    ) AS _FIVETRAN_SYNCED
  FROM
    {{ source(
      'algorand',
      'TXN'
    ) }}
  WHERE
    txid IS NULL
    AND ROUND > (
      SELECT
        MAX(ROUND)
      FROM
        {{ source(
          'algorand',
          'TXN'
        ) }}
    )
),
emptyROUNDS AS(
  SELECT
    *
  FROM
    emptyROUNDS_fivetran
  UNION
  SELECT
    *
  FROM
    emptyROUNDS_hevo
),
fulljson_fivetran AS (
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
),
fulljson_hevo AS (
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
    AND ROUND > (
      SELECT
        MAX(ROUND)
      FROM
        {{ source(
          'algorand',
          'TXN'
        ) }}
    )
),
fulljson AS(
  SELECT
    *
  FROM
    fulljson_fivetran
  UNION
  SELECT
    *
  FROM
    fulljson_hevo
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
