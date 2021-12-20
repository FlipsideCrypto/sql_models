{{ config(
    materialized = 'view',
    tags = ['snowflake', 'algorand_views', 'account', 'gold'],
) }}

SELECT
    address,
    account_closed,
    rewardsbase,
    balance,
    closed_at,
    created_at,
    wallet_type,
    account_data
FROM
    {{ ref('silver_algorand__account') }}
