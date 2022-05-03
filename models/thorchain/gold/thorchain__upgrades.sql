{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = "CONCAT_WS('-', block_id, from_address, to_address, burn_asset)",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'silver_thorchain', 'upgrades']
) }}
--total_block_rewards
WITH block_prices AS (

  SELECT
    AVG(rune_usd) AS rune_usd,
    block_id
  FROM
    {{ ref('silver_thorchain__prices') }}
  GROUP BY
    block_id
)
SELECT
  block_timestamp,
  se.block_id,
  from_address,
  to_address,
  burn_asset,
  burn_e8 / pow(
    10,
    8
  ) AS rune_amount,
  burn_e8 / pow(
    10,
    8
  ) * rune_usd AS rune_amount_usd
FROM
  {{ ref('silver_thorchain__switch_events') }}
  se
  LEFT JOIN block_prices p
  ON se.block_id = p.block_id
        {% if is_incremental() %}
WHERE se.block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}