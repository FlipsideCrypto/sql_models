{{ config(
  materialized = 'incremental',
  unique_key = '_unique_key',
  incremental_strategy = 'merge',
  tags = ['snowflake', 'algorand', 'transaction_participation', 'silver_algorand']
) }}

SELECT
  ROUND AS block_id,
  intra,
  algorand_decode_hex_addr(
    addr :: text
  ) AS address,
  concat_ws(
    '-',
    block_id :: STRING,
    intra :: STRING,
    address :: STRING
  ) AS _unique_key,
  _FIVETRAN_SYNCED
FROM
  {{ source(
    'algorand',
    'TXN_PARTICIPATION'
  ) }}
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
