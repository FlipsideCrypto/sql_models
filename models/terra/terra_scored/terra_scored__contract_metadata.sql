{{ config(
  materialized = 'view',
  tags = ['snowflake', 'terra_views', 'terra', 'contract_metadata']
) }}

WITH dex_contracts AS (
    SELECT
    block_timestamp,
    tx_id,
    contract_address,
    decimals,
    contract_name,
    symbol,
    FALSE AS is_wormhole
    FROM {{ ref('silver_terra__dex_contracts') }}
)

SELECT
block_timestamp,
tx_id,
contract_address,
decimals,
contract_name,
symbol,
is_wormhole
FROM {{ ref('silver_terra__contract_metadata') }}
WHERE contract_address NOT IN (SELECT contract_address FROM dex_contracts)

UNION ALL 

SELECT
block_timestamp,
tx_id,
contract_address,
decimals,
contract_name,
symbol,
is_wormhole
FROM dex_contracts
