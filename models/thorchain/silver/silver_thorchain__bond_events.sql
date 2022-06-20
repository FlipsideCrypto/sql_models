{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'bond_events']
) }}

WITH base AS (
  SELECT 
	b.tx,
	b.from_addr,
	b.asset_e8,
	b.bond_type,
	b.e8,
	b.block_timestamp,
	b.to_addr,
	b.chain,
	b.asset,
	b.memo,
	p.cnt,
    ROW_NUMBER() OVER (PARTITION BY b.tx, b.from_addr, b.asset_e8, b.bond_type, b.e8, b.block_timestamp, b.to_addr, b.chain, b.asset, b.memo ORDER BY p.cnt) AS rn
  FROM
    {{ ref('thorchain_dbt__bond_events') }}
    b
    JOIN {{ ref('thorchain_dbt__bond_events_pk_count') }}
    p
    ON p.tx = b.tx 
    AND p.from_addr = b.from_addr
    AND p.asset_e8 = b.asset_e8
    AND p.bond_type = b.bond_type
    AND p.e8 = b.e8
    AND p.block_timestamp = b.block_timestamp
    AND p.to_addr = b.to_addr
    AND p.chain = b.chain
    AND p.asset = b.asset
    AND p.memo = b.memo

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
  asset_e8,
  bond_type,
  e8,
  block_timestamp,
  to_addr,
  chain,
  asset,
  memo
FROM
  base
  WHERE rn <= cnt
