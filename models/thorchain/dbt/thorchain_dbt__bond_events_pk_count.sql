{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain_dbt', 'bond_events']
) }}

SELECT
  *
FROM
  {{ source(
    'thorchain_midgard',
    'bond_events_pk_count'
  ) }}

{% if is_incremental() %}
AND __HEVO_loaded_at >= (
  SELECT
    MAX(__HEVO_loaded_at)
  FROM
    {{ this }}
)
{% endif %}
