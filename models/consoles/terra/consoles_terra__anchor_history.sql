{{ config(
    materialized = 'view',
    unique_key = 'date',
    tags = ['snowflake', 'terra', 'anchor', 'console', 'anchor_history']
) }}

WITH borrows AS (

    SELECT
        TRUNC(
            block_timestamp,
            'day'
        ) AS DATE,
        COUNT(
            DISTINCT tx_id
        ) AS n_tx,
        SUM(amount) AS total_borrowed,
        COUNT(
            DISTINCT sender
        ) AS borrowers
    FROM
        {{ ref('anchor__borrows') }}
    GROUP BY
        DATE
),
repayments AS (
    SELECT
        TRUNC(
            block_timestamp,
            'day'
        ) AS DATE,
        COUNT(
            DISTINCT tx_id
        ) AS n_tx,
        SUM(amount) AS total_repaid,
        COUNT(
            DISTINCT sender
        ) AS repayers
    FROM
        {{ ref('anchor__repay') }}
    WHERE currency = 'uusd'
    GROUP BY
        DATE
),

liquidations AS (
    SELECT
        TRUNC(
            block_timestamp,
            'day'
        ) AS DATE,
        COUNT(
            DISTINCT tx_id
        ) AS n_tx,
        SUM(liquidated_amount_usd) AS amount,
        'Liquidation' AS description
    FROM
        {{ ref('anchor__liquidations') }}
    GROUP BY
        DATE
),
average_daily_luna_prices AS (
    SELECT
        TRUNC(
            block_timestamp,
            'day'
        ) AS DATE,
        AVG(price_usd) AS price_usd
    FROM
        {{ ref('terra__oracle_prices') }}
    WHERE
        currency = 'uluna'
    GROUP BY
        DATE
)
SELECT
    borrows.date,
    borrows.n_tx AS number_of_borrows,
    borrows.total_borrowed,
    repayments.n_tx AS number_of_repayments,
    repayments.total_repaid,
    IFF(
        liquidations.n_tx IS NULL,
        0,
        liquidations.n_tx
    ) AS number_of_liquidations,
    IFF(
        liquidations.amount IS NULL,
        0,
        liquidations.amount
    ) AS total_liquidated,
    borrows.total_borrowed - repayments.total_repaid - IFF(
        liquidations.amount IS NULL,
        0,
        liquidations.amount
    ) AS debt_added,
    average_daily_luna_prices.price_usd
FROM
    borrows
    JOIN repayments
    ON borrows.date = repayments.date
    JOIN average_daily_luna_prices
    ON borrows.date = average_daily_luna_prices.date
    LEFT JOIN liquidations
    ON borrows.date = liquidations.date
ORDER BY
    DATE DESC
