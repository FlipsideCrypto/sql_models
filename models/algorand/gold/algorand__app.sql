{{ config(
    materialized = 'view',
    tags = ['snowflake', 'algorand_views', 'app', 'gold'],
) }}

SELECT
    app_id,
    creator_address,
    app_closed,
    closed_at,
    created_at,
    params
FROM
    {{ ref('silver_algorand__app') }}
