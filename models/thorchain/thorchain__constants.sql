{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'thorchain', 'constants']
  )
}}

SELECT
  c.key,
  c.value
FROM {{source('thorchain_midgard', 'constants')}} c