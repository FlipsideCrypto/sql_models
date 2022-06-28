{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'fee_events']
) }}

WITH base AS (
  SELECT 
    f.asset,
    f.asset_e8,
    f.pool_deduct,
    f.block_timestamp,
    f.tx,
    p.cnt,
    ROW_NUMBER() OVER (PARTITION BY f.asset, f.asset_e8, f.pool_deduct, f.block_timestamp, f.tx ORDER BY p.cnt) AS rn
  FROM
    {{ ref('thorchain_dbt__fee_events') }}
    f
    LEFT JOIN {{ ref('thorchain_dbt__fee_events_pk_count') }}
    p
    ON p.asset = f.asset 
    AND p.asset_e8 = f.asset_e8
    AND p.pool_deduct = f.pool_deduct
    AND p.block_timestamp = f.block_timestamp
    AND p.tx = f.tx

	{% if is_incremental() %}
	WHERE
	__HEVO_loaded_at >= (
		SELECT
		MAX(__HEVO_loaded_at)
		FROM
		{{ this }}
	)
	{% endif %}
)
SELECT 
  asset,
  asset_e8,
  pool_deduct,
  block_timestamp,
  tx
FROM
  base
  WHERE rn <= COALESCE(cnt, 1)
