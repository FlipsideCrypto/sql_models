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
    algorand_decode_hex_addr(
      addr :: text
    ) AS address
  FROM
    {{ source(
      'algorand',
      'TXN_PARTICIPATION'
    ) }}

{% if is_incremental() %}
WHERE
  _FIVETRAN_SYNCED >= (
    SELECT
      MAX(
        _INSERTED_TIMESTAMP
      )
    FROM
      {{ this }}
  )
{% endif %}
GROUP BY
  block_id,
  intra,
  address
),
hevo_inner_tx_individual AS(
  SELECT
    ROUND AS block_id,
    intra,
    algorand_decode_hex_addr(
      addr :: text
    ) AS address
  FROM
    {{ source(
      'algorand',
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

{% if is_incremental() %}
WHERE
  _FIVETRAN_SYNCED >= (
    SELECT
      MAX(
        _INSERTED_TIMESTAMP
      )
    FROM
      {{ this }}
  )
{% endif %}
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
  address,
  concat_ws(
    '-',
    iti.block_id :: STRING,
    iti.intra :: STRING,
    iti.address :: STRING
  ) AS _unique_key,
  SYSDATE() AS _INSERTED_TIMESTAMP
FROM
  all_inner_tx_individual iti
  LEFT JOIN {{ ref('silver_algorand__block') }}
  ab
  ON iti.block_id = ab.block_id
