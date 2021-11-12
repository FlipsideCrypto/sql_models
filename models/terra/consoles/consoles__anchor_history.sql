-- Velocity: 61ef8e66-1b59-4c68-b451-524aee171182
{{ config(
    materialized = 'incremental',
    unique_key = 'date',
    incremental_strategy = 'delete+insert',
    tags = ['snowflake', 'terra', 'anchor', 'console']
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
        SUM(
            msg_value :execute_msg :borrow_stable :borrow_amount
        ) / 1e6 AS total_borrowed,
        COUNT(
            DISTINCT msg_value :sender
        ) AS borrowers
    FROM
        {{ ref('terra__msgs') }}
    WHERE
        msg_value :contract = 'terra1sepfj7s0aeg5967uxnfk4thzlerrsktkpelm5s'
        AND msg_value :execute_msg :borrow_stable IS NOT NULL
        AND tx_status = 'SUCCEEDED'

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
    SELECT
        MAX(
            block_timestamp :: DATE
        )
    FROM
        {{ ref('terra__msgs') }}
)
{% endif %}
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
        SUM(
            msg_value :coins [0] :amount
        ) / 1e6 AS total_repaid,
        COUNT(
            DISTINCT msg_value :sender
        ) AS repayers
    FROM
        {{ ref('terra__msgs') }}
    WHERE
        msg_value :contract = 'terra1sepfj7s0aeg5967uxnfk4thzlerrsktkpelm5s'
        AND msg_value :coins [0] :denom = 'uusd'
        AND msg_value :execute_msg :repay_stable IS NOT NULL
        AND tx_status = 'SUCCEEDED'

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
    SELECT
        MAX(
            block_timestamp :: DATE
        )
    FROM
        {{ ref('terra__msgs') }}
)
{% endif %}
GROUP BY
    DATE
),
liquidate_tx AS (
    SELECT
        tx_id
    FROM
        {{ ref('terra__msgs') }}
    WHERE
        msg_value :contract = 'terra1tmnqgvg567ypvsvk6rwsga3srp7e3lg6u0elp8'
        AND msg_value :execute_msg :liquidate_collateral IS NOT NULL
        AND tx_status = 'SUCCEEDED'

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
    SELECT
        MAX(
            block_timestamp :: DATE
        )
    FROM
        {{ ref('terra__msgs') }}
)
{% endif %}
),
repayment_by_liquidations AS (
    SELECT
        TRUNC(
            block_timestamp,
            'day'
        ) AS DATE,
        COUNT(
            DISTINCT tx_id
        ) AS n_tx,
        SUM(
            event_attributes :"1_amount" [0] :amount
        ) / 1e6 AS amount,
        'Repayment By Liquidation' AS description
    FROM
        {{ ref('terra__msg_events') }}
    WHERE
        tx_id IN (
            SELECT
                tx_id
            FROM
                liquidate_tx
        )
        AND event_type = 'transfer'
        AND event_attributes :"1_recipient" = 'terra1sepfj7s0aeg5967uxnfk4thzlerrsktkpelm5s'

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
    SELECT
        MAX(
            block_timestamp :: DATE
        )
    FROM
        {{ ref('terra__msg_events') }}
)
{% endif %}
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

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
    SELECT
        MAX(
            block_timestamp :: DATE
        )
    FROM
        {{ ref('terra__oracle_prices') }}
)
{% endif %}
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
        repayment_by_liquidations.n_tx IS NULL,
        0,
        repayment_by_liquidations.n_tx
    ) AS number_of_liquidations,
    IFF(
        repayment_by_liquidations.amount IS NULL,
        0,
        repayment_by_liquidations.amount
    ) AS total_liquidated,
    borrows.total_borrowed - repayments.total_repaid - IFF(
        repayment_by_liquidations.amount IS NULL,
        0,
        repayment_by_liquidations.amount
    ) AS debt_added,
    average_daily_luna_prices.price_usd
FROM
    borrows
    JOIN repayments
    ON borrows.date = repayments.date
    JOIN average_daily_luna_prices
    ON borrows.date = average_daily_luna_prices.date
    LEFT JOIN repayment_by_liquidations
    ON borrows.date = repayment_by_liquidations.date
ORDER BY
    DATE DESC
