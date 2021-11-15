{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain', 'unstake_events']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e._FIVETRAN_ID AS event_id,
  e.tx AS tx_id,
  e.chain AS blockchain,
  e.pool AS pool_name,
  e.to_addr AS to_address,
  e.from_addr AS from_address,
  e.asset,
  e.emit_rune_e8,
  e.asymmetry,
  e.asset_e8,
  e.stake_units,
  e.memo,
  e.emit_asset_e8,
  e.imp_loss_protection_e8,
  e.basis_points,
  e._EMIT_ASSET_IN_RUNE_E8
FROM
  {{ source(
    'thorchain_midgard',
    'unstake_events'
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
