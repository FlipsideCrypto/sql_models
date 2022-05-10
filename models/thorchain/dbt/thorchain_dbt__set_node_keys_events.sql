{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain_dbt', 'fee_set_node_keys_eventsevents']
) }}

SELECT
  *
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_set_node_keys_events'
  ) }}

{% if is_incremental() %}
AND __HEVO_loaded_at >= (
  SELECT
    MAX(__HEVO_loaded_at)
  FROM
    {{ this }}
)
{% endif %}
