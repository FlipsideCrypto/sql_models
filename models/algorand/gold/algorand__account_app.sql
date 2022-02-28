{{ config(
    materialized = 'view',
    tags = ['snowflake', 'algorand_views', 'account_app', 'gold'],
) }}

SELECT
    address,
    app_id,
    app_closed,
    closed_at,
    created_at,
    created_at_timestamp,
    app_info
FROM
    {{ ref('silver_algorand__account_app') }}
