{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'switch_events']
) }}

WITH base AS (
  SELECT 
	s.tx,
	s.from_addr,
	s.to_addr,
	s.burn_asset,
	s.burn_e8,
	s.mint_e8,
	s.block_timestamp,
	p.cnt,
    ROW_NUMBER() OVER (PARTITION BY s.tx, s.from_addr, s.to_addr, s.burn_asset, s.burn_e8, s.mint_e8, s.block_timestamp ORDER BY p.cnt) AS rn
  FROM
    {{ ref('thorchain_dbt__switch_events') }}
    s
    JOIN {{ ref('thorchain_dbt__switch_events_pk_count') }}
    p
    ON p.tx = s.tx 
    AND p.from_addr = s.from_addr
    AND p.to_addr = s.to_addr
    AND p.burn_asset = s.burn_asset
    AND p.burn_e8 = s.burn_e8
    AND p.mint_e8 = s.mint_e8
    AND p.block_timestamp = s.block_timestamp
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
  tx,
  from_addr,
  to_addr,
  burn_asset,
  burn_e8,
  mint_e8,
  block_timestamp
FROM
  base
  WHERE rn <= cnt
