{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain_dbt', 'rewards_event_entries']
) }}

SELECT
  *
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_rewards_event_entries'
  ) }}

{% if is_incremental() %}
AND __HEVO_loaded_at >= (
  SELECT
    MAX(__HEVO_loaded_at)
  FROM
    {{ this }}
)
{% endif %}
