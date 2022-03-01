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
    extra
  FROM
    {{ source(
      'algorand',
      'TXN'
    ) }}
  WHERE
    txid IS NULL

{% if is_incremental() %}
AND __HEVO__LOADED_AT >= (
  SELECT
    MAX(
      _INSERTED_TIMESTAMP
    )
  FROM
    {{ this }}
)
{% endif %}
),
emptyROUNDS_hevo AS (
  SELECT
    ROUND,
    intra,
    txn,
    extra
  FROM
    {{ source(
      'algorand',
      'TXN_MISSING'
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

{% if is_incremental() %}
AND __HEVO__LOADED_AT >= (
  SELECT
    MAX(
      _INSERTED_TIMESTAMP
    )
  FROM
    {{ this }}
)
{% endif %}
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

{% if is_incremental() %}
AND __HEVO__LOADED_AT >= (
  SELECT
    MAX(
      _INSERTED_TIMESTAMP
    )
  FROM
    {{ this }}
)
{% endif %}
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
      'TXN_MISSING'
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

{% if is_incremental() %}
AND __HEVO__LOADED_AT >= (
  SELECT
    MAX(
      _INSERTED_TIMESTAMP
    )
  FROM
    {{ this }}
)
{% endif %}
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
  SYSDATE() AS _inserted_timestamp
FROM
  emptyROUNDS er
  LEFT JOIN fulljson f
  ON er.extra :"root-intra" :: NUMBER = f.intra
  AND er.round = f.round
WHERE
  f.round IS NOT NULL
