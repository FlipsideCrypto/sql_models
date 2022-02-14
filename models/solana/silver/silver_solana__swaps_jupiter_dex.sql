{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', block_id, tx_id)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['snowflake', 'solana', 'silver_solana', 'solana_swaps']
) }}

WITH jupiter_dex_txs AS (

    SELECT
        DISTINCT i.block_id,
        i.block_timestamp,
        i.tx_id,
        t.fee,
        t.account_keys,
        t.succeeded
    FROM
        {{ ref('solana_dbt__instructions') }}
        i
        LEFT OUTER JOIN {{ ref('silver_solana__transactions') }}
        t
        ON t.tx_id = i.tx_id
    WHERE
        i.value :programId :: STRING = 'JUP2jxvXaqu7NQY1GmNF4m1vodw12LVXYxbFL2uJvfo' -- jupiter aggregator v2

{% if is_incremental() %}
AND i.ingested_at >= CURRENT_DATE - 2
AND t.ingested_at >= CURRENT_DATE - 2
{% endif %}
),
destinations AS (
    SELECT
        i.block_id,
        i.block_timestamp,
        i.tx_id,
        i.succeeded,
        i.index,
        ii.index AS inner_index,
        ii.value :parsed :info :destination :: STRING AS destination,
        ii.value :parsed :info :amount AS amount,
        ROW_NUMBER() over (
            PARTITION BY i.tx_id
            ORDER BY
                i.index,
                inner_index
        ) AS rn
    FROM
        {{ ref('silver_solana__events') }}
        i
        INNER JOIN jupiter_dex_txs t
        ON t.tx_id = i.tx_id
        LEFT OUTER JOIN TABLE(FLATTEN(inner_instruction :instructions)) ii
    WHERE
        ii.value :parsed :info :destination :: STRING IS NOT NULL
        AND COALESCE(
            ii.value :programId :: STRING,
            ''
        ) <> '11111111111111111111111111111111'

{% if is_incremental() %}
AND i.ingested_at >= CURRENT_DATE - 2
{% endif %}
),
post_balances_acct_map AS (
    SELECT
        t.tx_id,
        t.account_keys [b.account_index] :pubkey :: STRING AS middle_acct,
        b.owner,
        b.mint,
        b.decimal,
        b.amount
    FROM
        {{ ref('solana_dbt__post_token_balances') }}
        b
        INNER JOIN jupiter_dex_txs t
        ON t.tx_id = b.tx_id

{% if is_incremental() %}
WHERE
    b.ingested_at >= CURRENT_DATE - 2
{% endif %}
),
pre_balances_acct_map AS (
    SELECT
        t.tx_id,
        t.account_keys [b.account_index] :pubkey :: STRING AS middle_acct,
        b.owner,
        b.mint,
        b.decimal,
        b.amount
    FROM
        {{ ref('solana_dbt__pre_token_balances') }}
        b
        INNER JOIN jupiter_dex_txs t
        ON t.tx_id = b.tx_id

{% if is_incremental() %}
WHERE
    b.ingested_at >= CURRENT_DATE - 2
{% endif %}
),
signers AS (
    SELECT
        t.tx_id,
        A.value :pubkey :: STRING AS acct,
        A.index
    FROM
        jupiter_dex_txs t,
        TABLE(FLATTEN(t.account_keys)) A
    WHERE
        A.value :signer = TRUE
),
signers_info AS (
    SELECT
        t.tx_id,
        t.fee,
        s.acct,
        b.sol_pre_balances [s.index] AS signer_sol_pre_balance,
        b.sol_post_balances [s.index] AS signer_sol_post_balance
    FROM
        {{ ref('solana_dbt__sol_balances') }}
        b
        INNER JOIN jupiter_dex_txs t
        ON t.tx_id = b.tx_id
        LEFT OUTER JOIN signers s
        ON s.tx_id = b.tx_id

{% if is_incremental() %}
WHERE
    b.ingested_at >= CURRENT_DATE - 2
{% endif %}
),
swap_actions AS (
    SELECT
        d.*,
        m.owner,
        m.mint,
        m.decimal,
        MAX(rn) over (
            PARTITION BY d.tx_id
        ) AS max_rn
    FROM
        destinations d
        LEFT OUTER JOIN post_balances_acct_map m
        ON d.tx_id = m.tx_id
        AND d.destination = m.middle_acct
    WHERE
        COALESCE(
            d.amount :: STRING,
            '-1'
        ) <> '0'
),
swaps_tmp AS (
    SELECT
        *
    FROM
        swap_actions
    WHERE
        (
            rn = 1
            OR rn = max_rn
        )
)
SELECT
    s1.tx_id,
    s1.block_id,
    s1.block_timestamp,
    s1.succeeded,
    COALESCE(
        s2.owner,
        s.acct
    ) AS swapper,
    COALESCE(
        GREATEST(
            s1.amount * pow(
                10,- s1.decimal
            ),
            COALESCE(
                pre_from.amount - post_from.amount,
                0
            ) * pow(
                10,- s1.decimal
            )
        ),
        (
            s.signer_sol_pre_balance - s.signer_sol_post_balance + s.fee
        ) * pow(
            10,
            -9
        )
    ) AS swap_from_amount,
    COALESCE(
        s1.mint,
        'So11111111111111111111111111111111111111112'
    ) AS swap_from_mint,
    COALESCE(
        GREATEST(
            s2.amount * pow(
                10,- s2.decimal
            ),
            COALESCE(
                post_to.amount - pre_to.amount,
                0
            ) * pow(
                10,- s2.decimal
            )
        ),
        (
            s.signer_sol_post_balance - s.signer_sol_pre_balance + s.fee
        ) * pow(
            10,
            -9
        )
    ) AS swap_to_amount,
    COALESCE(
        s2.mint,
        'So11111111111111111111111111111111111111112'
    ) AS swap_to_mint
FROM
    swaps_tmp s1
    LEFT OUTER JOIN swaps_tmp s2
    ON s1.tx_id = s2.tx_id
    AND s1.rn <> s2.rn
    LEFT OUTER JOIN signers_info s
    ON s.tx_id = s1.tx_id
    LEFT OUTER JOIN pre_balances_acct_map pre_from
    ON pre_from.tx_id = s1.tx_id
    AND pre_from.mint = s1.mint
    AND pre_from.owner = COALESCE(
        s2.owner,
        s.acct
    )
    LEFT OUTER JOIN pre_balances_acct_map pre_to
    ON pre_to.tx_id = s1.tx_id
    AND pre_to.mint = s2.mint
    AND pre_to.owner = COALESCE(
        s2.owner,
        s.acct
    )
    LEFT OUTER JOIN post_balances_acct_map post_from
    ON post_from.tx_id = s1.tx_id
    AND post_from.mint = s1.mint
    AND post_from.owner = COALESCE(
        s2.owner,
        s.acct
    )
    LEFT OUTER JOIN post_balances_acct_map post_to
    ON post_to.tx_id = s1.tx_id
    AND post_to.mint = s2.mint
    AND post_to.owner = COALESCE(
        s2.owner,
        s.acct
    )
WHERE
    s1.rn = 1
