{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain_dbt', 'validator_request_leave_events']
) }}

SELECT
  *
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_validator_request_leave_events'
  ) }}

{% if is_incremental() %}
AND __HEVO_loaded_at >= (
  SELECT
    MAX(__HEVO_loaded_at)
  FROM
    {{ this }}
)
{% endif %}
