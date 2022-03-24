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
    __HEVO__LOADED_AT AS _INSERTED_TIMESTAMP
  FROM
    {{ source(
      'algorand',
      'TXN_PARTICIPATION'
    ) }}
  WHERE
    ROUND > 18993228
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
    intra :: STRING,
    address :: STRING
  ) AS _unique_key,
  iti._INSERTED_TIMESTAMP AS _INSERTED_TIMESTAMP
FROM
  inner_tx_individual iti
  LEFT JOIN {{ ref('silver_algorand__block') }}
  ab
  ON iti.block_id = ab.block_id
WHERE
  1 = 1

{% if is_incremental() %}
AND iti._INSERTED_TIMESTAMP >= (
  SELECT
    MAX(
      _INSERTED_TIMESTAMP
    )
  FROM
    {{ this }}
)
OR block_timestamp IS NULL
{% endif %}
