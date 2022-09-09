{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['date'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

WITH address_ranges AS (

    SELECT
        A.address,
        b.block_date AS min_block_date,
        CURRENT_TIMESTAMP :: DATE AS max_block_date
    FROM
        {{ ref('silver__account') }} A
        JOIN {{ ref('silver__block') }}
        b
        ON A.created_at = b.block_id
),
cte_my_date AS (
    SELECT
        HOUR :: DATE AS DATE
    FROM
        {{ source(
            'shared',
            'hours'
        ) }}
    WHERE
        HOUR :: DATE <= CURRENT_DATE :: DATE

{% if is_incremental() %}
AND HOUR :: DATE >=(
    SELECT
        DATEADD('day', -2, MAX(DATE))
    FROM
        {{ this }})
    {% endif %}
    GROUP BY
        DATE
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
        A.sender,
        A.receiver,
        amount,
        A.block_id,
        A.intra,
        b.block_timestamp,
        tx_type
    FROM
        {{ ref('silver__transaction') }} A
        JOIN {{ ref('silver__block') }}
        b
        ON A.block_id = b.block_id
),
senderpay AS(
    SELECT
        A.sender AS address,
        ((A.amount * -1) -.001) AS amount,
        A.block_id,
        A.intra,
        A.block_timestamp
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
        A.block_timestamp
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
        A.block_timestamp
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
        ) AS amount,
        A.block_id,
        A.intra,
        C.block_timestamp
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
                WHEN asa.decimals > 0 THEN A.amount / pow(
                    10,
                    asa.decimals
                )
                WHEN asa.decimals = 0 THEN A.amount
            END AS amount,
            A.block_id,
            A.intra,
            C.block_timestamp
        FROM
            {{ ref('silver__transaction_close') }} A
            JOIN address_ranges b
            ON A.account = b.address
            LEFT JOIN {{ ref('silver__asset') }}
            asa
            ON A.asset_id = asa.asset_id
            JOIN {{ ref('silver__block') }} C
            ON A.block_id = C.block_id
        WHERE
            A.asset_id = 0

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
        block_timestamp
    FROM
        senderpay
    UNION ALL
    SELECT
        address,
        amount,
        block_id,
        intra,
        block_timestamp
    FROM
        sendersfee
    UNION ALL
    SELECT
        address,
        amount,
        block_id,
        intra,
        block_timestamp
    FROM
        receivers
    UNION ALL
    SELECT
        address,
        amount,
        block_id,
        intra,
        block_timestamp
    FROM
        reward
    UNION ALL
    SELECT
        address,
        amount,
        block_id,
        intra,
        block_timestamp
    FROM
        closes
    UNION ALL
    SELECT
        address,
        0 amount,
        1 block_id,
        0 intra,
        min_block_date AS block_timestamp
    FROM
        address_ranges
),
dailysummed_balances AS(
    SELECT
        block_timestamp :: DATE AS DATE,
        address,
        SUM(amount) AS amount
    FROM
        all_actions
    GROUP BY
        block_timestamp :: DATE,
        address
),
rollup_balances AS (
    SELECT
        DATE,
        address,
        SUM(amount) over (
            PARTITION BY address
            ORDER BY
                DATE
        ) AS balance
    FROM
        (
            SELECT
                DATE,
                address,
                SUM(amount) amount
            FROM
                (
                    SELECT
                        DATE,
                        address,
                        amount
                    FROM
                        dailysummed_balances

{% if is_incremental() %}
UNION ALL
SELECT
    DATE,
    address,
    balance AS amount
FROM
    {{ this }}
WHERE
    DATE :: DATE < (
        SELECT
            DATEADD('day', -2, MAX(DATE))
        FROM
            {{ this }}) qualify (ROW_NUMBER() over(PARTITION BY address
        ORDER BY
            DATE DESC) = 1)
        {% endif %}
    ) z
GROUP BY
    DATE,
    address
) x
),
balance_tmp AS (
    SELECT
        d.date,
        d.address AS address,
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
    LAST_VALUE(
        balance ignore nulls
    ) over(
        PARTITION BY address
        ORDER BY
            DATE ASC rows unbounded preceding
    ) AS balance,
    concat_ws(
        '-',
        address,
        DATE
    ) AS _unique_key
FROM
    balance_tmp
ORDER BY
    address,
    DATE DESC
