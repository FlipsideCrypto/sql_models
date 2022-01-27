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
