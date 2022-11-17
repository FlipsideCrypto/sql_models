{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['date']
) }}

WITH address_ranges AS (

    SELECT
        DISTINCT A.address,
        b.block_date AS min_block_date,
        CURRENT_TIMESTAMP :: DATE AS max_block_date
    FROM
        {{ ref('silver__account') }} A
        JOIN {{ ref('silver__block') }}
        b
        ON A.created_at = b.block_id
        JOIN {{ ref('silver__pool_addresses') }} C
        ON A.address = C.address
    WHERE
        b.block_date :: DATE >= '2022-01-15'
),
cte_my_date AS (
    SELECT
        HOUR DATE
    FROM
        {{ source(
            'shared',
            'hours'
        ) }}
    WHERE
        HOUR :: DATE <= CURRENT_DATE :: DATE
        AND HOUR :: DATE >= '2022-01-15'

{% if is_incremental() %}
AND HOUR >=(
    SELECT
        DATEADD('day', -2, MAX(DATE))
    FROM
        {{ this }})
    {% endif %}
),
all_dates AS (
    SELECT
        C.date,
        A.address
    FROM
        cte_my_date C
        JOIN address_ranges A
        ON C.date BETWEEN A.min_block_date
        AND A.max_block_date
),
txns AS (
    SELECT
        COALESCE(
            A.asset_sender,
            A.sender
        ) AS sender,
        A.receiver,
        asset_receiver,
        CASE
            WHEN A.asset_id = 0 THEN COALESCE(
                A.amount,
                A.asset_amount
            ) / pow(
                10,
                6
            )
            WHEN asa.decimals > 0 THEN COALESCE(
                A.amount,
                A.asset_amount
            ) / pow(
                10,
                asa.decimals
            )
            ELSE COALESCE(
                A.amount,
                A.asset_amount
            )
        END AS amount,
        A.block_id,
        A.intra,
        b.block_timestamp,
        tx_type,
        A.asset_id
    FROM
        {{ ref('silver__transaction') }} A
        JOIN {{ ref('silver__block') }}
        b
        ON A.block_id = b.block_id
        LEFT JOIN {{ ref('silver__asset') }}
        asa
        ON A.asset_id = asa.asset_id
),
senderpay AS(
    SELECT
        A.sender AS address,
        ((A.amount * -1) -.001) AS amount,
        A.block_id,
        A.intra,
        A.block_timestamp,
        0 AS asset_id
    FROM
        txns A
        JOIN address_ranges b
        ON A.sender = b.address
    WHERE
        tx_type = 'pay'

{% if is_incremental() %}
AND block_timestamp :: DATE >=(
    SELECT
        DATEADD('day', -2, MAX(DATE))
    FROM
        {{ this }})
    {% endif %}
),
sendersfee AS(
    SELECT
        A.sender AS address,
        0.001 * -1 AS amount,
        A.block_id,
        A.intra,
        A.block_timestamp,
        0 AS asset_id
    FROM
        txns A
        JOIN address_ranges b
        ON A.sender = b.address
    WHERE
        tx_type <> 'pay'

{% if is_incremental() %}
AND block_timestamp :: DATE >=(
    SELECT
        DATEADD('day', -2, MAX(DATE))
    FROM
        {{ this }})
    {% endif %}
),
receivers AS (
    SELECT
        A.receiver AS address,
        A.amount,
        A.block_id,
        A.intra,
        A.block_timestamp,
        0 AS asset_id
    FROM
        txns A
        JOIN address_ranges b
        ON A.receiver = b.address
    WHERE
        tx_type = 'pay'

{% if is_incremental() %}
AND block_timestamp :: DATE >=(
    SELECT
        DATEADD('day', -2, MAX(DATE))
    FROM
        {{ this }})
    {% endif %}
),
reward AS (
    SELECT
        A.account AS address,
        A.amount / pow(
            10,
            6
        ) amount,
        A.block_id,
        A.intra,
        C.block_timestamp,
        0 AS asset_id
    FROM
        {{ ref('silver__transaction_reward') }} A
        JOIN address_ranges b
        ON A.account = b.address
        JOIN {{ ref('silver__block') }} C
        ON A.block_id = C.block_id

{% if is_incremental() %}
WHERE
    block_timestamp :: DATE >=(
        SELECT
            DATEADD('day', -2, MAX(DATE))
        FROM
            {{ this }})
        {% endif %}
    ),
    closes AS (
        SELECT
            A.account AS address,
            CASE
                WHEN A.asset_id = 0 THEN A.amount / pow(
                    10,
                    6
                )
                WHEN asa.decimals > 0 THEN A.amount / pow(
                    10,
                    asa.decimals
                )
                ELSE A.amount
            END amount,
            A.block_id,
            A.intra,
            C.block_timestamp,
            A.asset_id
        FROM
            {{ ref('silver__transaction_close') }} A
            JOIN address_ranges b
            ON A.account = b.address
            LEFT JOIN {{ ref('silver__asset') }}
            asa
            ON A.asset_id = asa.asset_id
            JOIN {{ ref('silver__block') }} C
            ON A.block_id = C.block_id

{% if is_incremental() %}
AND block_timestamp :: DATE >=(
    SELECT
        DATEADD('day', -2, MAX(DATE))
    FROM
        {{ this }})
    {% endif %}
),
closes_send AS (
    SELECT
        tx.address address,
        CASE
            WHEN A.asset_id = 0 THEN A.amount / pow(
                10,
                6
            )
            WHEN asa.decimals > 0 THEN A.amount / pow(
                10,
                asa.decimals
            )
            ELSE A.amount
        END amount,
        A.block_id,
        A.intra,
        C.block_timestamp,
        A.asset_id
    FROM
        {{ ref('silver__transaction_close') }} A
        JOIN (
            SELECT
                DISTINCT block_id,
                intra,
                sender AS address
            FROM
                txns
        ) tx
        ON A.block_id = tx.block_id
        AND A.intra = tx.intra
        JOIN address_ranges b
        ON tx.address = b.address
        LEFT JOIN {{ ref('silver__asset') }}
        asa
        ON A.asset_id = asa.asset_id
        JOIN {{ ref('silver__block') }} C
        ON A.block_id = C.block_id

{% if is_incremental() %}
AND block_timestamp :: DATE >=(
    SELECT
        DATEADD('day', -2, MAX(DATE))
    FROM
        {{ this }})
    {% endif %}
),
senderasset AS(
    SELECT
        A.sender AS address,
        A.amount * -1 AS amount,
        A.block_id,
        A.intra,
        A.block_timestamp,
        A.asset_id
    FROM
        txns A
        JOIN address_ranges b
        ON A.sender = b.address
    WHERE
        tx_type = 'axfer'

{% if is_incremental() %}
AND block_timestamp :: DATE >=(
    SELECT
        DATEADD('day', -2, MAX(DATE))
    FROM
        {{ this }})
    {% endif %}
),
receiversasset AS (
    SELECT
        A.asset_receiver AS address,
        A.amount AS amount,
        A.block_id,
        A.intra,
        A.block_timestamp,
        A.asset_id
    FROM
        txns A
        JOIN address_ranges b
        ON A.asset_receiver = b.address
    WHERE
        tx_type = 'axfer'

{% if is_incremental() %}
AND block_timestamp :: DATE >=(
    SELECT
        DATEADD('day', -2, MAX(DATE))
    FROM
        {{ this }})
    {% endif %}
),
all_actions AS(
    SELECT
        address,
        amount,
        block_id,
        intra,
        block_timestamp,
        asset_id
    FROM
        senderpay
    UNION ALL
    SELECT
        address,
        amount,
        block_id,
        intra,
        block_timestamp,
        asset_id
    FROM
        sendersfee
    UNION ALL
    SELECT
        address,
        amount,
        block_id,
        intra,
        block_timestamp,
        asset_id
    FROM
        receivers
    UNION ALL
    SELECT
        address,
        amount,
        block_id,
        intra,
        block_timestamp,
        asset_id
    FROM
        reward
    UNION ALL
    SELECT
        address,
        amount,
        block_id,
        intra,
        block_timestamp,
        asset_id
    FROM
        closes
    UNION ALL
    SELECT
        address,
        amount,
        block_id,
        intra,
        block_timestamp,
        asset_id
    FROM
        closes_send
    UNION ALL
    SELECT
        address,
        0 amount,
        1 block_id,
        0 intra,
        min_block_date AS block_timestamp,
        0 asset_id
    FROM
        address_ranges
    UNION ALL
    SELECT
        address,
        amount,
        block_id,
        intra,
        block_timestamp,
        asset_id
    FROM
        senderasset
    UNION ALL
    SELECT
        address,
        amount,
        block_id,
        intra,
        block_timestamp,
        asset_id
    FROM
        receiversasset
),
dailysummed_balances AS(
    SELECT
        DATE_TRUNC(
            'HOUR',
            block_timestamp
        ) AS DATE,
        address,
        asset_id,
        SUM(amount) AS amount
    FROM
        all_actions
    GROUP BY
        DATE_TRUNC(
            'HOUR',
            block_timestamp
        ),
        address,
        asset_id
),
asset_hours AS (
    SELECT
        DISTINCT A.date,
        A.address,
        b.asset_id
    FROM
        all_dates A
        JOIN all_actions b
        ON A.address = b.address
),
rollup_balances AS (
    SELECT
        DATE,
        address,
        asset_id,
        SUM(COALESCE(amount, 0)) over (
            PARTITION BY address,
            asset_id
            ORDER BY
                DATE
        ) AS balance
    FROM
        (
            SELECT
                DATE,
                address,
                asset_id,
                SUM(amount) amount
            FROM
                (
                    SELECT
                        ah.date,
                        ah.address,
                        ah.asset_id,
                        COALESCE(
                            amount,
                            0
                        ) amount
                    FROM
                        asset_hours ah
                        LEFT JOIN dailysummed_balances x
                        ON ah.date = x.date
                        AND ah.address = x.address
                        AND ah.asset_id = x.asset_id

{% if is_incremental() %}
UNION ALL
SELECT
    DATE,
    address,
    asset_id,
    balance AS amount
FROM
    {{ this }}
WHERE
    DATE :: DATE < (
        SELECT
            DATEADD('day', -2, MAX(DATE))
        FROM
            {{ this }}) qualify (ROW_NUMBER() over(PARTITION BY address, asset_id
        ORDER BY
            DATE DESC) = 1)
        {% endif %}
    ) z
GROUP BY
    DATE,
    address,
    asset_id
) x
),
balance_tmp AS (
    SELECT
        d.date,
        d.address AS address,
        asset_id,
        b.balance
    FROM
        (
            SELECT
                DATE,
                address
            FROM
                all_dates
            UNION
            SELECT
                DATE,
                address
            FROM
                rollup_balances
        ) d
        LEFT JOIN rollup_balances b
        ON d.date = b.date
        AND d.address = b.address
)
SELECT
    DATE,
    address,
    asset_id,
    LAST_VALUE(
        balance ignore nulls
    ) over(
        PARTITION BY address,
        asset_id
        ORDER BY
            DATE ASC rows unbounded preceding
    ) AS balance,
    concat_ws(
        '-',
        address,
        asset_id,
        DATE
    ) AS _unique_key
FROM
    balance_tmp
ORDER BY
    address,
    asset_id,
    DATE DESC
