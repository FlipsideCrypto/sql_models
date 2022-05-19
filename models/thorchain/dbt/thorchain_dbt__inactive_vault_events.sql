{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain_dbt', 'inactive_vault_events']
) }}

SELECT
  *
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_inactive_vault_events'
  ) }}
