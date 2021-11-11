{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain', 'stake_events']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e._FIVETRAN_ID AS event_id,
  e.owner,
  e.chain,
  e.address,
  e.expire,
  e.name,
  e.fund_amount_e8,
  e.registration_fee_e8
FROM
  {{ source(
    'thorchain_midgard',
    'thorname_change_events'
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
