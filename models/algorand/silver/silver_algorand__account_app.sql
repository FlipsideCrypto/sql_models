{{ config(
  materialized = 'incremental',
  unique_key = '_unique_key',
  incremental_strategy = 'merge',
  tags = ['snowflake', 'algorand', 'account_app']
) }}

SELECT
  addr :: STRING AS address,
  app AS app_id,
  deleted AS app_closed,
  closed_at AS closed_at,
  created_at AS created_at,
  localstate AS app_info,
  concat_ws(
    '-',
    addr :: STRING,
    app :: STRING
  ) AS _unique_key,
  _FIVETRAN_SYNCED
FROM
  {{ source(
    'algorand',
    'ACCOUNT_APP'
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
