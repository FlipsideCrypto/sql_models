{{ config(
  materialized = 'table',
  unique_key = 'day',
  tags = ['snowflake', 'thorchain', 'total_value_locked']
) }}

WITH bond_type_day AS (

  SELECT
    DATE(block_timestamp) AS DAY,
    bond_type,
    (SUM(e8) / pow(10, 8)) AS rune_amount
  FROM
    {{ ref('silver_thorchain__bond_events') }}

{% if is_incremental() %}
WHERE
  block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
GROUP BY
  DAY,
  bond_type
),
bond_type_day_direction AS (
  SELECT
    DAY,
    bond_type,
    CASE
      WHEN bond_type IN (
        'bond_returned',
        'bond_cost'
      ) THEN -1
      ELSE 1
    END AS direction,
    rune_amount,
    rune_amount * direction AS abs_rune_amount
  FROM
    bond_type_day

{% if is_incremental() %}
WHERE
  DAY >= getdate() - INTERVAL '5 days'
{% endif %}
),
total_value_bonded_tbl AS (
  SELECT
    DAY,
    SUM(abs_rune_amount) AS total_value_bonded
  FROM
    bond_type_day_direction

{% if is_incremental() %}
WHERE
  DAY >= getdate() - INTERVAL '5 days'
{% endif %}
GROUP BY
  1
),
total_pool_depth AS (
  SELECT
    DATE(block_timestamp) AS DAY,
    block_id,
    pool_name,
    rune_e8,
    asset_e8,
    MAX(block_id) over (PARTITION BY pool_name, DATE(block_timestamp)) AS max_block_id
  FROM
    {{ ref('silver_thorchain__block_pool_depths') }}

{% if is_incremental() %}
WHERE
  block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
),
total_pool_depth_max AS (
  SELECT
    DAY,
    rune_e8 AS rune_depth,
    asset_e8 AS asset_depth
  FROM
    total_pool_depth
  WHERE
    block_id = max_block_id

{% if is_incremental() %}
AND DAY >= getdate() - INTERVAL '5 days'
{% endif %}
),
total_value_pooled_tbl AS (
  SELECT
    DAY,
    SUM(rune_depth) * 2 / power(
      10,
      8
    ) AS total_value_pooled
  FROM
    total_pool_depth_max

{% if is_incremental() %}
WHERE
  DAY >= getdate() - INTERVAL '5 days'
{% endif %}
GROUP BY
  1
)
SELECT
  COALESCE(
    total_value_bonded_tbl.day,
    total_value_pooled_tbl.day
  ) AS DAY,
  COALESCE(
    total_value_pooled,
    0
  ) AS total_value_pooled,
  COALESCE(SUM(total_value_bonded) over (
ORDER BY
  COALESCE(total_value_bonded_tbl.day, total_value_pooled_tbl.day) ASC), 0) AS total_value_bonded,
  COALESCE(
    total_value_pooled,
    0
  ) + SUM(COALESCE(total_value_bonded, 0)) over (
    ORDER BY
      COALESCE(
        total_value_bonded_tbl.day,
        total_value_pooled_tbl.day
      ) ASC
  ) AS total_value_locked
FROM
  total_value_bonded_tbl full
  JOIN total_value_pooled_tbl
  ON total_value_bonded_tbl.day = total_value_pooled_tbl.day
ORDER BY
  1 DESC
