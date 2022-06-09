{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', address, date)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['date'],
) }}

WITH address_ranges AS (

    SELECT
        DISTINCT A.address,
        b.block_timestamp :: DATE AS min_block_date,
        CURRENT_TIMESTAMP :: DATE AS max_block_date
    FROM
        {{ ref('silver_algorand__account') }} A
        JOIN {{ ref('silver_algorand__block') }}
        b
        ON A.created_at = b.block_id
    WHERE
        1 = 1 -- AND address = 'AZ2KKAHF2PJMEEUVN4E2ILMNJCSZLJJYVLBIA7HOY3BQ7AENOVVTXMGN3I'
        -- AND address IN (
        --     'BQZA4KC2TGQS27X3ZS4VVJSH3PZD3CZXGSYN5OXPQJLHZ6VBT5IVKT7FKU',
        --     'ZCHLS5Q23KHCND2X6GQWUL3YXJ2K2CMYN2RTKOKX6R4TMZYCDY7ABQRHRA',
        --     '6EWTIUDZGKHUOHK6H3HKRLWD4DKUVJIQ3ENFPPA476LHWM7BMA3TS6NSSQ',
        --     'BQZA4KC2TGQS27X3ZS4VVJSH3PZD3CZXGSYN5OXPQJLHZ6VBT5IVKT7FKU',
        --     'DO4EJV2M2YMFPZA64QMPTJN6SMJBRMBNFBL4MDB44KQ2TJ46LSYKKYCUMQ',
        --     'AZ2KKAHF2PJMEEUVN4E2ILMNJCSZLJJYVLBIA7HOY3BQ7AENOVVTXMGN3I'
        -- )
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
senderpay AS(
    SELECT
        A.sender AS address,
        ((A.amount * -1) -.001) AS amount,
        A.block_id,
        A.intra,
        A.block_timestamp
    FROM
        {{ ref('silver_algorand__payment_transaction') }} A
        JOIN address_ranges b
        ON A.sender = b.address

{% if is_incremental() %}
WHERE
    block_timestamp :: DATE >=(
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
            {{ ref('silver_algorand__transactions') }} A
            JOIN address_ranges b
            ON A.sender = b.address
        WHERE
            A.tx_type <> 'pay'

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
        {{ ref('silver_algorand__payment_transaction') }} A
        JOIN address_ranges b
        ON A.receiver = b.address

{% if is_incremental() %}
WHERE
    block_timestamp :: DATE >=(
        SELECT
            DATEADD('day', -2, MAX(DATE))
        FROM
            {{ this }})
        {% endif %}
    ),
    reward AS (
        SELECT
            A.account AS address,
            A.amount,
            A.block_id,
            A.intra,
            A.block_timestamp
        FROM
            {{ ref('silver_algorand__transaction_rewards') }} A
            JOIN address_ranges b
            ON A.account = b.address

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
            A.amount,
            A.block_id,
            A.intra,
            A.block_timestamp
        FROM
            {{ ref('silver_algorand__transaction_closes') }} A
            JOIN address_ranges b
            ON A.account = b.address

{% if is_incremental() %}
WHERE
    block_timestamp :: DATE >=(
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
    ) AS balance
FROM
    balance_tmp
ORDER BY
    address,
    DATE DESC
