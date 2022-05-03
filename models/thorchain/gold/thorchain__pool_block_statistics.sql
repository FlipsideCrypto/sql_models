{{ config(
  materialized = 'table',
  unique_key = "CONCAT_WS('-', day, asset)",
  tags = ['snowflake', 'thorchain', 'pool_block_statistics']
) }}

WITH pool_depth AS (

  SELECT
    DAY,
    pool_name,
    rune_e8 AS rune_depth,
    asset_e8 AS asset_depth,
    rune_e8 / asset_e8 AS asset_price
  FROM
    (
      SELECT
        DATE(block_timestamp) AS DAY,
        block_id,
        pool_name,
        rune_e8,
        asset_e8,
        MAX(block_id) over (PARTITION BY pool_name, DATE(block_timestamp)) AS max_block_id
      FROM
        {{ ref("silver_thorchain__block_pool_depths") }}
      WHERE
        asset_e8 > 0

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
)
WHERE
  block_id = max_block_id
),
pool_status AS (
  SELECT
    DAY,
    pool_name,
    status
  FROM
    (
      SELECT
        DATE(block_timestamp) AS DAY,
        asset AS pool_name,
        status,
        ROW_NUMBER() over (PARTITION BY pool_name, DATE(block_timestamp)
      ORDER BY
        block_timestamp DESC, status) AS rn
      FROM
        {{ ref("silver_thorchain__pool_events") }}

{% if is_incremental() %}
WHERE
  block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
)
WHERE
  rn = 1
),
add_liquidity_tbl AS (
  SELECT
    DATE(block_timestamp) AS DAY,
    pool_name,
    COUNT(*) AS add_liquidity_count,
    SUM(rune_e8) AS add_rune_liquidity_volume,
    SUM(asset_e8) AS add_asset_liquidity_volume,
    SUM(stake_units) AS added_stake
  FROM
    {{ ref("silver_thorchain__stake_events") }}

{% if is_incremental() %}
WHERE
  block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
GROUP BY
  1,
  2
),
withdraw_tbl AS (
  SELECT
    DATE(block_timestamp) AS DAY,
    pool_name,
    COUNT(*) AS withdraw_count,
    SUM(emit_rune_e8) AS withdraw_rune_volume,
    SUM(emit_asset_e8) AS withdraw_asset_volume,
    SUM(stake_units) AS withdrawn_stake,
    SUM(imp_loss_protection_e8) AS impermanent_loss_protection_paid
  FROM
    {{ ref("silver_thorchain__unstake_events") }}

{% if is_incremental() %}
WHERE
  block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
GROUP BY
  1,
  2
),
swap_total_tbl AS (
  SELECT
    DAY,
    pool_name,
    SUM(volume) AS swap_volume
  FROM
    (
      SELECT
        DATE(block_timestamp) AS DAY,
        pool_name,
        CASE
          WHEN to_asset = 'THOR.RUNE' THEN to_e8
          ELSE from_e8
        END AS volume
      FROM
        {{ ref("silver_thorchain__swap_events") }}

{% if is_incremental() %}
WHERE
  block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
)
GROUP BY
  1,
  2
),
swap_to_asset_tbl AS (
  SELECT
    DAY,
    pool_name,
    SUM(liq_fee_in_rune_e8) AS to_asset_fees,
    SUM(from_e8) AS to_asset_volume,
    COUNT(*) AS to_asset_count,
    AVG(swap_slip_bp) AS to_asset_average_slip
  FROM(
      SELECT
        DATE(block_timestamp) AS DAY,
        pool_name,
        CASE
          WHEN to_asset = 'THOR.RUNE' THEN 'to_rune'
          ELSE 'to_asset'
        END AS to_tune_asset,
        liq_fee_in_rune_e8,
        to_e8,
        from_e8,
        swap_slip_bp,
        CASE
          WHEN to_asset = 'THOR.RUNE' THEN 0
          ELSE liq_fee_e8
        END AS asset_fee
      FROM
        {{ ref("silver_thorchain__swap_events") }}

{% if is_incremental() %}
WHERE
  block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
)
GROUP BY
  to_tune_asset,
  pool_name,
  DAY
HAVING
  to_tune_asset = 'to_asset'
),
swap_to_rune_tbl AS (
  SELECT
    DAY,
    pool_name,
    SUM(liq_fee_in_rune_e8) AS to_rune_fees,
    SUM(to_e8) AS to_rune_volume,
    COUNT(*) AS to_rune_count,
    AVG(swap_slip_bp) AS to_rune_average_slip
  FROM(
      SELECT
        DATE(block_timestamp) AS DAY,
        pool_name,
        CASE
          WHEN to_asset = 'THOR.RUNE' THEN 'to_rune'
          ELSE 'to_asset'
        END AS to_tune_asset,
        liq_fee_in_rune_e8,
        to_e8,
        from_e8,
        swap_slip_bp,
        CASE
          WHEN to_asset = 'THOR.RUNE' THEN 0
          ELSE liq_fee_e8
        END AS asset_fee
      FROM
        {{ ref("silver_thorchain__swap_events") }}

{% if is_incremental() %}
WHERE
  block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
)
GROUP BY
  to_tune_asset,
  pool_name,
  DAY
HAVING
  to_tune_asset = 'to_rune'
),
average_slip_tbl AS (
  SELECT
    DATE(block_timestamp) AS DAY,
    pool_name,
    AVG(swap_slip_bp) AS average_slip
  FROM
    {{ ref("silver_thorchain__swap_events") }}

{% if is_incremental() %}
WHERE
  block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
GROUP BY
  pool_name,
  DAY
),
-- // https://gitlab.com/thorchain/midgard/-/merge_requests/28/diffs
unique_swapper_tbl AS (
  SELECT
    DATE(block_timestamp) AS DAY,
    pool_name,
    COUNT(
      DISTINCT from_address
    ) AS unique_swapper_count
  FROM
    {{ ref("silver_thorchain__swap_events") }}

{% if is_incremental() %}
WHERE
  block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
GROUP BY
  pool_name,
  DAY
),
stake_amount AS (
  SELECT
    DATE(block_timestamp) AS DAY,
    pool_name,
    SUM(stake_units) AS units
  FROM
    {{ ref("silver_thorchain__stake_events") }}

{% if is_incremental() %}
WHERE
  block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
GROUP BY
  pool_name,
  DAY
),
unstake_umc AS (
  SELECT
    DATE(block_timestamp) AS DAY,
    from_address AS address,
    pool_name,
    SUM(stake_units) AS unstake_liquidity_units
  FROM
    {{ ref("silver_thorchain__unstake_events") }}

{% if is_incremental() %}
WHERE
  block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
GROUP BY
  from_address,
  pool_name,
  DAY
),
stake_umc AS (
  SELECT
    DATE(block_timestamp) AS DAY,
    rune_address AS address,
    pool_name,
    SUM(stake_units) AS liquidity_units
  FROM
    {{ ref("silver_thorchain__stake_events") }}
  WHERE
    rune_address IS NOT NULL

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
GROUP BY
  rune_address,
  pool_name,
  DAY
UNION ALL
SELECT
  DATE(block_timestamp) AS DAY,
  asset_address AS address,
  pool_name,
  SUM(stake_units) AS liquidity_units
FROM
  {{ ref("silver_thorchain__stake_events") }}
WHERE
  asset_address IS NOT NULL
  AND rune_address IS NULL

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
GROUP BY
  asset_address,
  pool_name,
  DAY
),
unique_member_count AS (
  SELECT
    DAY,
    pool_name,
    COUNT(
      DISTINCT address
    ) AS unique_member_count
  FROM
    (
      SELECT
        stake_umc.day,
        stake_umc.pool_name,
        stake_umc.address,
        stake_umc.liquidity_units,
        CASE
          WHEN unstake_umc.unstake_liquidity_units IS NOT NULL THEN unstake_umc.unstake_liquidity_units
          ELSE 0
        END AS unstake_liquidity_units
      FROM
        stake_umc
        LEFT JOIN unstake_umc
        ON stake_umc.address = unstake_umc.address
        AND stake_umc.pool_name = unstake_umc.pool_name
    )
  WHERE
    liquidity_units - unstake_liquidity_units > 0
  GROUP BY
    pool_name,
    DAY
),
asset_price_usd_tbl AS (
  SELECT
    DAY,
    pool_name,
    asset_usd AS asset_price_usd
  FROM
    (
      SELECT
        DATE(block_timestamp) AS DAY,
        block_id,
        MAX(block_id) over (PARTITION BY pool_name, DATE(block_timestamp)) AS max_block_id,
        pool_name,
        asset_usd
      FROM
        {{ ref("silver_thorchain__prices") }}
    )
  WHERE
    block_id = max_block_id
),
joined AS (
  SELECT
    pool_depth.day AS DAY,
    COALESCE(
      add_asset_liquidity_volume,
      0
    ) AS add_asset_liquidity_volume,
    COALESCE(
      add_liquidity_count,
      0
    ) AS add_liquidity_count,
    COALESCE(
      (
        add_asset_liquidity_volume + add_rune_liquidity_volume
      ),
      0
    ) AS add_liquidity_volume,
    COALESCE(
      add_rune_liquidity_volume,
      0
    ) AS add_rune_liquidity_volume,
    pool_depth.pool_name AS asset,
    asset_depth,
    COALESCE(
      asset_price,
      0
    ) AS asset_price,
    COALESCE(
      asset_price_usd,
      0
    ) AS asset_price_usd,
    COALESCE(
      average_slip,
      0
    ) AS average_slip,
    COALESCE(
      impermanent_loss_protection_paid,
      0
    ) AS impermanent_loss_protection_paid,
    COALESCE(
      rune_depth,
      0
    ) AS rune_depth,
    COALESCE(
      status,
      'no status'
    ) AS status,
    COALESCE((to_rune_count + to_asset_count), 0) AS swap_count,
    COALESCE(
      swap_volume,
      0
    ) AS swap_volume,
    COALESCE(
      to_asset_average_slip,
      0
    ) AS to_asset_average_slip,
    COALESCE(
      to_asset_count,
      0
    ) AS to_asset_count,
    COALESCE(
      to_asset_fees,
      0
    ) AS to_asset_fees,
    COALESCE(
      to_asset_volume,
      0
    ) AS to_asset_volume,
    COALESCE(
      to_rune_average_slip,
      0
    ) AS to_rune_average_slip,
    COALESCE(
      to_rune_count,
      0
    ) AS to_rune_count,
    COALESCE(
      to_rune_fees,
      0
    ) AS to_rune_fees,
    COALESCE(
      to_rune_volume,
      0
    ) AS to_rune_volume,
    COALESCE((to_rune_fees + to_asset_fees), 0) AS totalFees,
    COALESCE(
      unique_member_count,
      0
    ) AS unique_member_count,
    COALESCE(
      unique_swapper_count,
      0
    ) AS unique_swapper_count,
    COALESCE(
      units,
      0
    ) AS units,
    COALESCE(
      withdraw_asset_volume,
      0
    ) AS withdraw_asset_volume,
    COALESCE(
      withdraw_count,
      0
    ) AS withdraw_count,
    COALESCE(
      withdraw_rune_volume,
      0
    ) AS withdraw_rune_volume,
    COALESCE((withdraw_rune_volume + withdraw_asset_volume), 0) AS withdraw_volume,
    SUM(COALESCE(added_stake, 0) - COALESCE(withdrawn_stake, 0)) over (
      PARTITION BY pool_depth.pool_name
      ORDER BY
        pool_depth.day ASC
    ) AS total_stake,
    asset_depth * COALESCE(
      rune_depth,
      0
    ) AS depth_product,
    CASE
      WHEN total_stake = 0 THEN 0
      ELSE SQRT(depth_product) / total_stake
    END AS liquidity_unit_value_index
  FROM
    pool_depth
    LEFT JOIN pool_status
    ON pool_depth.pool_name = pool_status.pool_name
    AND pool_depth.day = pool_status.day
    LEFT JOIN add_liquidity_tbl
    ON pool_depth.pool_name = add_liquidity_tbl.pool_name
    AND pool_depth.day = add_liquidity_tbl.day
    LEFT JOIN withdraw_tbl
    ON pool_depth.pool_name = withdraw_tbl.pool_name
    AND pool_depth.day = withdraw_tbl.day
    LEFT JOIN swap_total_tbl
    ON pool_depth.pool_name = swap_total_tbl.pool_name
    AND pool_depth.day = swap_total_tbl.day
    LEFT JOIN swap_to_asset_tbl
    ON pool_depth.pool_name = swap_to_asset_tbl.pool_name
    AND pool_depth.day = swap_to_asset_tbl.day
    LEFT JOIN swap_to_rune_tbl
    ON pool_depth.pool_name = swap_to_rune_tbl.pool_name
    AND pool_depth.day = swap_to_rune_tbl.day
    LEFT JOIN unique_swapper_tbl
    ON pool_depth.pool_name = unique_swapper_tbl.pool_name
    AND pool_depth.day = unique_swapper_tbl.day
    LEFT JOIN stake_amount
    ON pool_depth.pool_name = stake_amount.pool_name
    AND pool_depth.day = stake_amount.day
    LEFT JOIN average_slip_tbl
    ON pool_depth.pool_name = average_slip_tbl.pool_name
    AND pool_depth.day = average_slip_tbl.day
    LEFT JOIN unique_member_count
    ON pool_depth.pool_name = unique_member_count.pool_name
    AND pool_depth.day = unique_member_count.day
    LEFT JOIN asset_price_usd_tbl
    ON pool_depth.pool_name = asset_price_usd_tbl.pool_name
    AND pool_depth.day = asset_price_usd_tbl.day
)
SELECT
  DISTINCT DAY,
  add_asset_liquidity_volume,
  add_liquidity_count,
  add_liquidity_volume,
  add_rune_liquidity_volume,
  asset,
  asset_depth,
  asset_price,
  asset_price_usd,
  average_slip,
  impermanent_loss_protection_paid,
  rune_depth,
  status,
  swap_count,
  swap_volume,
  to_asset_average_slip,
  to_asset_count,
  to_asset_fees,
  to_asset_volume,
  to_rune_average_slip,
  to_rune_count,
  to_rune_fees,
  to_rune_volume,
  totalFees,
  unique_member_count,
  unique_swapper_count,
  units,
  withdraw_asset_volume,
  withdraw_count,
  withdraw_rune_volume,
  withdraw_volume,
  total_stake,
  depth_product,
  liquidity_unit_value_index,
  LAG(
    liquidity_unit_value_index,
    1
  ) over (
    PARTITION BY asset
    ORDER BY
      DAY ASC
  ) AS prev_liquidity_unit_value_index
FROM
  joined
