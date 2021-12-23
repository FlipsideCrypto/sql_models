{{ config(
    materialized = 'incremental',
    unique_key = 'balance_date || address_name',
    incremental_strategy = 'delete+insert',
    tags = ['snowflake', 'ethereum', 'aave', 'aave_safety_module', 'atb_test']
) }}

WITH safety_module AS (

    SELECT
        *
    FROM
        {{ source(
            'bronze',
            'prod_ethereum_sink_407559501'
        ) }}
    WHERE
        record_content :model :class = 'eth.contract_read_model.ContractReadModel'
        AND record_content :model.run_id = 'v2021.12.17.0'

{% if is_incremental() %}
AND (
    record_metadata :CreateTime :: INT / 1000
) :: TIMESTAMP :: DATE >= (
    SELECT
        DATEADD('day', -1, MAX(balance_date :: DATE))
    FROM
        {{ this }}
)
{% endif %}
),
balance AS (
    SELECT
        (
            record_metadata :CreateTime :: INT / 1000
        ) :: TIMESTAMP AS system_created_at,
        t.value :block_id :: bigint AS block_id,
        t.value :block_timestamp :: TIMESTAMP AS block_timestamp,
        t.value :contract_address :: STRING AS contract_address,
        t.value :value_numeric :: numeric / power(
            10,
            18
        ) AS value_numeric
    FROM
        safety_module,
        LATERAL FLATTEN(
            input => record_content :results
        ) t
),
prices AS (
    SELECT
        DISTINCT DATE_TRUNC(
            'day',
            recorded_at
        ) AS block_date,
        LOWER(symbol) AS symbol,
        AVG(price) AS price
    FROM
        {{ source(
            'shared',
            'prices_v2'
        ) }}
    WHERE
        asset_id = 'staked-aave-balancer-pool-token'

{% if is_incremental() %}
AND recorded_at >= getdate() - INTERVAL '45 days'
{% endif %}
GROUP BY
    block_date,
    symbol
),
abpt AS (
    SELECT
        DATE_TRUNC(
            'day',
            block_timestamp
        ) AS balance_date,
        AVG(value_numeric) AS balance
    FROM
        balance
    GROUP BY
        balance_date
),
stkaave AS (
    SELECT
        balance_date,
        address_name,
        AVG(price) AS price,
        AVG(balance) AS balance,
        AVG(amount_usd) AS amount_usd
    FROM
        {{ ref('ethereum__erc20_balances') }}
    WHERE
        (LOWER(user_address) = LOWER('0x4da27a545c0c5b758a6ba100e3a049001de870f5'))

{% if is_incremental() %}
AND balance_date >= getdate() - INTERVAL '45 days'
{% endif %}
GROUP BY
    balance_date,
    address_name
),
pool1 AS (
    SELECT
        b.balance_date,
        p.symbol AS address_name,
        p.price AS price,
        b.balance AS balance,
        p.price * b.balance AS amount_usd
    FROM
        abpt b
        LEFT JOIN prices p
        ON b.balance_date = p.block_date
)
SELECT
    balance_date,
    address_name,
    price,
    balance,
    amount_usd
FROM
    pool1
WHERE
    address_name IS NOT NULL
    AND price IS NOT NULL
    AND balance_date > '2021-06-29'
UNION ALL
SELECT
    balance_date,
    address_name,
    price,
    balance,
    amount_usd
FROM
    stkaave
WHERE
    address_name IS NOT NULL
    AND price IS NOT NULL
    AND balance_date > '2021-06-29'
