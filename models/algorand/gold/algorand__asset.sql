{{ config(
    materialized = 'view',
    tags = ['snowflake', 'algorand_views', 'asset', 'gold'],
) }}

SELECT
    asset_id,
    creator_address,
    total_supply,
    asset_name,
    asset_url,
    decimals,
    asset_deleted,
    closed_at,
    created_at
FROM
    {{ ref('silver_algorand__asset') }}
UNION
SELECT
    0 AS asset_id,
    'NULL' AS creator_address,
    10000000000 AS total_supply,
    'ALGO' AS asset_name,
    'https://algorand.foundation/' AS asset_url,
    6 AS decimals,
    'FALSE' AS asset_deleted,
    NULL AS closed_at,
    0 AS created_at
