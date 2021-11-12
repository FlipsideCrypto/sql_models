-- Velocity: 320abd12-4abf-4c05-a64a-a541f49e8c5c
{{ config(
    materialized = 'incremental',
    unique_key = 'date',
    incremental_strategy = 'delete+insert',
    tags = ['snowflake', 'terra', 'console']
) }}

SELECT
    DATE,
    SUM(balance_usd) AS staked_supply_usd
FROM
    {{ ref('terra__daily_balances') }}
WHERE
    currency = 'LUNA'
    AND balance_type = 'staked'

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
    SELECT
        MAX(
            block_timestamp :: DATE
        )
    FROM
        {{ ref('terra__daily_balances') }}
)
{% endif %}
GROUP BY
    DATE
ORDER BY
    DATE DESC
