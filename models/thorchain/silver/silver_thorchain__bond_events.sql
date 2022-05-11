{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'bond_events']
) }}

SELECT
  *
FROM
  {{ ref('thorchain_dbt__bond_events') }}

{% if is_incremental() %}
AND __HEVO_loaded_at >= (
  SELECT
    MAX(__HEVO_loaded_at)
  FROM
    {{ this }}
)
{% endif %}
