{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain', 'switch_events']
) }}

SELECT
  _FIVETRAN_ID AS identified_id,
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.burn_asset,
  e.burn_e8,
  e.to_addr AS to_address,
  e.from_addr AS from_address,
  e.tx
FROM
  {{ source(
    'thorchain_midgard',
    'switch_events'
  ) }}
  e
  INNER JOIN {{ source(
    'thorchain_midgard',
    'block_log'
  ) }}
  bl
  ON bl.timestamp = e.block_timestamp
WHERE
  (
    e._FIVETRAN_DELETED IS NULL
    OR e._FIVETRAN_DELETED = FALSE
  )
GROUP BY
  identified_id,
  block_timestamp,
  block_id,
  burn_asset,
  burn_e8,
  to_address,
  from_address
