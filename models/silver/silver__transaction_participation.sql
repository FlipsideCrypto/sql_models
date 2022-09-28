{{ config(
  materialized = 'incremental',
  unique_key = '_unique_key',
  incremental_strategy = 'merge',
  cluster_by = ['_INSERTED_TIMESTAMP::DATE']
) }}

WITH base AS(

  SELECT
    ROUND AS block_id,
    intra,
    addr :: text AS address_raw,
    DATEADD(
      ms,
      __HEVO__LOADED_AT,
      '1970-01-01'
    ) AS _INSERTED_TIMESTAMP
  FROM
    {{ source(
      'algorand',
      'TXN_PARTICIPATION'
    ) }}

{% if is_incremental() %}
WHERE
  _INSERTED_TIMESTAMP >= (
    SELECT
      MAX(
        _INSERTED_TIMESTAMP
      )
    FROM
      {{ this }}
  ) - INTERVAL '4 HOURS'
{% endif %}
)
SELECT
  A.block_id,
  A.intra,
  b.address,
  A._inserted_timestamp,
  concat_ws(
    '-',
    block_id,
    intra,
    address
  ) AS _unique_key
FROM
  base A
  JOIN {{ ref('silver__account') }}
  b
  ON A.address_raw = b.address_raw
