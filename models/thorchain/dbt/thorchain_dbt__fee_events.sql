{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain_dbt', 'fee_events']
) }}

SELECT
  *
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_fee_events'
  ) }}

{% if is_incremental() %}
AND __HEVO_loaded_at >= (
  SELECT
    MAX(__HEVO_loaded_at)
  FROM
    {{ this }}
)
{% endif %}
