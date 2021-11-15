{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = 'event_id',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'thorchain', 'thorchain_transfers']
) }}

WITH block_prices AS (

  SELECT
    AVG(rune_usd) AS rune_usd,
    block_id
  FROM
    {{ ref('thorchain__prices') }}
  GROUP BY
    block_id
)
SELECT
  block_timestamp,
  event_id,
  se.block_id,
  from_address,
  to_address,
  asset,
  COALESCE(amount_e8 / pow(10, 8), 0) AS rune_amount,
  COALESCE(amount_e8 / pow(10, 8) * rune_usd, 0) AS rune_amount_usd
FROM
  {{ ref('thorchain__transfer_events') }}
  se
  LEFT JOIN block_prices p
  ON se.block_id = p.block_id
