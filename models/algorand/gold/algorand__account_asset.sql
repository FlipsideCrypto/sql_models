{{ config(
    materialized = 'view',
    tags = ['snowflake', 'algorand_views', 'account_asset', 'gold'],
) }}

SELECT
    address,
    asset_id,
    asset_name,
    amount,
    asset_added_at,
    asset_last_removed,
    asset_closed,
    frozen
FROM
    {{ ref('silver_algorand__account_asset') }}
