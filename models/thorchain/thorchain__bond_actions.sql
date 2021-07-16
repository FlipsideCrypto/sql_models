{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key=["tx_id"], 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'thorchain', 'thorchain_bond_actions']
  )
}}

WITH block_prices AS (
  SELECT
    AVG(rune_usd) AS rune_usd,
    block_id
  FROM {{ ref('thorchain__prices') }}
  GROUP BY block_id
),

bond_events AS (
  SELECT * FROM {{ ref('thorchain__bond_events') }}
  WHERE TRUE
    {% if is_incremental() %}
    AND block_timestamp >= getdate() - interval '2 days'
    {% else %}
    AND block_timestamp >= getdate() - interval '9 months'
    {% endif %}
)

SELECT
  be.block_timestamp,
  be.block_id,
  tx_id,
  from_address,
  to_addres AS to_address,
  asset,
  blockchain,
  bond_type,
  e8 / POW(10, 8) AS asset_amount,
  rune_usd * asset_amount / POW(10, 8) AS asset_usd
FROM bond_events be

JOIN block_prices p 
ON be.block_id = p.block_id