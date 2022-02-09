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
    block_id :: STRING,
    intra :: STRING,
    address :: STRING
  ) AS _unique_key,
  _FIVETRAN_SYNCED
FROM
  inner_tx_individual iti
  LEFT JOIN {{ ref('silver_algorand__block') }}
  ab
  ON b.round = ab.block_id
WHERE
  1 = 1

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
