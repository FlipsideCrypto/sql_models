{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = "CONCAT_WS('-', block_id, reward_entity)",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'thorchain', 'thorchain_total_block_rewards']
) }}
--total_block_rewards
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
  ree.block_timestamp,
  ree.block_id,
  ree.pool_name AS reward_entity,
  COALESCE(rune_e8 / pow(10, 8), 0) AS rune_amount,
  COALESCE(rune_e8 / pow(10, 8) * rune_usd, 0) AS rune_amount_usd
FROM
  {{ ref('thorchain__rewards_event_entries') }}
  ree
  LEFT JOIN {{ ref('thorchain__prices') }}
  p
  ON ree.block_id = p.block_id
  AND ree.pool_name = p.pool_name
UNION
SELECT
  block_timestamp,
  re.block_id,
  'bond_holders' AS reward_entity,
  bond_e8 / pow(
    10,
    8
  ) AS rune_amount,
  bond_e8 / pow(
    10,
    8
  ) * rune_usd AS rune_amount_usd
FROM
  {{ ref('thorchain__rewards_events') }}
  re
  LEFT JOIN block_prices p
  ON re.block_id = p.block_id
