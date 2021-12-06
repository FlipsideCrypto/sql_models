{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', BLOCK_ID, INTRA, ADDRESS)",
  incremental_strategy = 'merge',
  tags = ['snowflake', 'algorand', 'transaction_participation']
) }}

SELECT
  ROUND AS block_id,
  intra,
  addr :: STRING AS address,
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
