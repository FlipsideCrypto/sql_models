{{ config(
  materialized = 'incremental',
  unique_key = '_unique_key',
  incremental_strategy = 'merge',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'algorand', 'transaction_participation', 'silver_algorand']
) }}

WITH inner_tx_individual AS(

  SELECT
    ROUND AS block_id,
    intra,
    addr :: text AS address,
    MIN(_FIVETRAN_SYNCED) AS _FIVETRAN_SYNCED
  FROM
    {{ source(
      'algorand',
      'TXN_PARTICIPATION'
    ) }}
  GROUP BY
    block_id,
    intra,
    address
),
hevo_inner_tx_individual AS(
  SELECT
    ROUND AS block_id,
    intra,
    addr :: text AS address,
    MIN(DATEADD('MS', __HEVO__LOADED_AT, '1970-01-01')) AS _FIVETRAN_SYNCED
  FROM
    {{ source(
      'algorand_patch',
      'TXN_PARTICIPATION_MISSING'
    ) }}
  WHERE
    ROUND > (
      SELECT
        MAX(ROUND)
      FROM
        {{ source(
          'algorand',
          'TXN_PARTICIPATION'
        ) }}
    )
  GROUP BY
    block_id,
    intra,
    address
),
all_inner_tx_individual AS(
  SELECT
    *
  FROM
    inner_tx_individual
  UNION
  SELECT
    *
  FROM
    hevo_inner_tx_individual
)
SELECT
  ab.block_timestamp AS block_timestamp,
  iti.block_id,
  iti.intra,
  algorand_decode_hex_addr(
    iti.address :: text
  ) AS address,
  concat_ws(
    '-',
    iti.block_id :: STRING,
    iti.intra :: STRING,
    iti.address :: STRING
  ) AS _unique_key,
  iti._FIVETRAN_SYNCED
FROM
  all_inner_tx_individual iti
  LEFT JOIN {{ ref('silver_algorand__block') }}
  ab
  ON iti.block_id = ab.block_id
WHERE
  1 = 1

{% if is_incremental() %}
AND iti._FIVETRAN_SYNCED >= (
  SELECT
    MAX(
      _FIVETRAN_SYNCED
    )
  FROM
    {{ this }}
)
{% endif %}
