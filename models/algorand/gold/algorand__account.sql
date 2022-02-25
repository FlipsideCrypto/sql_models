{{ config(
    materialized = 'view',
    tags = ['snowflake', 'algorand_views', 'account', 'gold'],
) }}

SELECT
    address,
    account_closed,
    rewardsbase,
    rewards_total,
    balance,
    closed_at,
    created_at,
    created_at_timestamp,
    wallet_type,
    account_data
FROM
    {{ ref('silver_algorand__account') }}
