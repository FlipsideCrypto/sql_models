{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain_dbt', 'set_ip_address_events']
) }}

SELECT
  *
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_set_ip_address_events'
  ) }}
