{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', block_id, tx_id)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
    persist_docs={"relation": true, "columns": true}, 
    tags = ['snowflake', 'solana', 'silver_solana', 'solana_swaps']
) }}

WITH base_i AS (

    SELECT
        *
    FROM
        {{ ref('solana_dbt__instructions') }}
        i

{% if is_incremental() %}
WHERE
    i.ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
),
orca_dex_txs AS (
    SELECT
        DISTINCT i.block_id,
        i.block_timestamp,
        i.tx_id,
        t.fee,
        t.account_keys,
        t.succeeded
    FROM
        base_i i
        INNER JOIN {{ ref('silver_solana__transactions') }}
        t
        ON t.tx_id = i.tx_id
    WHERE
        i.value :programId :: STRING IN (
            -- unknown orca swaps version, seems related to v2
            'MEV1HDn99aybER3U3oa9MySSXqoEZNDEQ4miAimTjaW',
            -- orca swaps v2
            '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP',
            -- orca swaps v1
            'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1'
        )

{% if is_incremental() %}
AND t.block_timestamp :: DATE >= CURRENT_DATE - 2
{% endif %}
),
delegates_map_tmp AS (
    SELECT
        i.tx_id,
        VALUE :parsed :info :delegate :: STRING AS delegate,
        VALUE :parsed :info :owner :: STRING AS owner
    FROM
        base_i i
    INNER JOIN orca_dex_txs t on t.tx_id = i.tx_id
    WHERE
        delegate IS NOT NULL
),
delegates_map as (
    select
        tx_id,
        delegate,
        owner
    from delegates_map_tmp
    group by 1,2,3
),
signers_tmp AS (
    SELECT
        t.tx_id,
        A.value :pubkey :: STRING AS acct,
        A.index
    FROM
        orca_dex_txs t
        LEFT OUTER JOIN TABLE(FLATTEN(t.account_keys)) A
    WHERE
        A.value :signer = TRUE
),
signers as (
    select 
        s.tx_id,
        s.acct,
        dm.owner AS delegate_owner
    from signers_tmp s
    LEFT OUTER JOIN delegates_map dm
    ON dm.tx_id = s.tx_id
    AND s.acct = dm.delegate
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
        INNER JOIN orca_dex_txs t
        ON t.tx_id = b.tx_id

{% if is_incremental() %}
WHERE
    b.ingested_at :: DATE >= CURRENT_DATE - 2
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
        ii.value :parsed :info :authority :: STRING AS authority,
        ii.value :parsed :info :source :: STRING AS source,
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
        INNER JOIN orca_dex_txs t
        ON t.tx_id = i.tx_id
        LEFT OUTER JOIN TABLE(FLATTEN(inner_instruction :instructions)) ii
    WHERE
        ii.value :parsed :info :destination :: STRING IS NOT NULL
        AND COALESCE(
            ii.value :programId :: STRING,
            ''
        ) <> '11111111111111111111111111111111'
        AND i.instruction :programId :: STRING IN (
            -- unknown orca swaps version, seems related to v2
            'MEV1HDn99aybER3U3oa9MySSXqoEZNDEQ4miAimTjaW',
            -- orca swaps v2
            '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP',
            -- orca swaps v1
            'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1'
        )

{% if is_incremental() %}
AND i.block_timestamp :: DATE >= CURRENT_DATE - 2
{% endif %}
),
destination_acct_map AS (
    SELECT
        tx_id,
        authority,
        source
    FROM
        destinations
    GROUP BY
        1,
        2,
        3
),
swaps_tmp_1 AS (
    SELECT
        COALESCE(
            s.delegate_owner,
            s.acct
        ) AS swapper,
        -- s.acct AS swapper,
        COALESCE(
            p1.owner,
            d2.authority
        ) AS destination_owner,
        COALESCE(
            p1.mint,
            p2.mint
        ) AS mint,
        COALESCE(
            p1.decimal,
            p2.decimal
        ) AS DECIMAL,
        d.*
    FROM
        destinations d
        LEFT OUTER JOIN signers s
        ON s.acct = d.authority
        and s.tx_id = d.tx_id
        LEFT OUTER JOIN post_balances_acct_map p1
        ON p1.middle_acct = d.destination
        AND p1.tx_id = d.tx_id
        LEFT OUTER JOIN post_balances_acct_map p2
        ON p2.middle_acct = d.source
        AND p2.tx_id = d.tx_id
        LEFT OUTER JOIN destination_acct_map d2
        ON d2.source = d.destination
        AND d2.tx_id = d.tx_id
),
swapper_min_rn AS (
    SELECT
        DISTINCT s.tx_id,
        MIN(rn) over (
            PARTITION BY s.tx_id
        ) AS min_swapper_rn
    FROM
        swaps_tmp_1 s
    WHERE
        swapper IS NOT NULL
),
swaps_tmp AS (
    SELECT
        s.block_id,
        s.block_timestamp,
        s.tx_id,
        s.succeeded,
        s.swapper,
        s.destination_owner,
        s.mint,
        s.decimal,
        s.index,
        s.inner_index,
        s.destination,
        s.authority,
        s.source,
        s.amount,
        ROW_NUMBER() over (
            PARTITION BY s.tx_id
            ORDER BY
                s.index,
                s.inner_index
        ) AS rn
    FROM
        swaps_tmp_1 s
        INNER JOIN swapper_min_rn m
        ON s.tx_id = m.tx_id
    WHERE
        s.rn >= m.min_swapper_rn
),
mint_acct_map AS (
    SELECT
        tx_id,
        source,
        mint,
        DECIMAL
    FROM
        swaps_tmp
    GROUP BY
        1,
        2,
        3,
        4
),
swap_actions AS (
    SELECT
        s1.block_id,
        s1.block_timestamp,
        s1.tx_id,
        s1.succeeded,
        s1.swapper,
        s1.destination_owner,
        s1.destination,
        s1.source,
        COALESCE(
            s1.mint,
            s2.mint
        ) AS mint,
        COALESCE(
            s1.decimal,
            s2.decimal
        ) AS DECIMAL,
        s1.amount :: bigint AS amount,
        s1.rn
    FROM
        swaps_tmp s1
        LEFT OUTER JOIN mint_acct_map s2
        ON s1.destination = s2.source
        AND s1.tx_id = s2.tx_id
),
swap_actions_with_refund AS (
    SELECT
        s1.*,
        s3.mint AS originating_mint,
        CASE
            WHEN s2.amount < s1.amount THEN s2.amount
            ELSE NULL
        END AS refund,
        MAX(refund) over (
            PARTITION BY s1.tx_id
        ) AS max_refund,
        s1.amount - COALESCE(
            refund,
            0
        ) AS final_amt
    FROM
        swap_actions s1
        LEFT OUTER JOIN swap_actions s2
        ON s1.tx_id = s2.tx_id
        AND s1.swapper = s2.destination_owner
        AND s1.mint = s2.mint
        AND s1.rn = 1
        LEFT OUTER JOIN swap_actions s3
        ON s1.tx_id = s3.tx_id
        AND s3.rn = 1
),
swap_actions_final AS (
    SELECT
        *
    FROM
        swap_actions_with_refund
    WHERE
        swapper IS NOT NULL
        OR mint <> originating_mint
        OR (
            originating_mint = mint
            AND max_refund IS NULL
        ) -- need to do this for situations where it appears the user swaps back to the same mint...
),
agg_tmp AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        succeeded,
        swapper,
        mint,
        DECIMAL,
        SUM(final_amt) AS amt,
        MIN(rn) AS rn
    FROM
        swap_actions_final
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7
),
agg AS (
    SELECT
        *,
        MAX(rn) over (
            PARTITION BY tx_id
        ) AS max_rn
    FROM
        agg_tmp
    WHERE
        amt <> 0
)
SELECT
    a1.block_id,
    a1.block_timestamp,
    a1.tx_id,
    a1.succeeded,
    a1.swapper,
    a1.mint AS from_mint,
    CASE
        WHEN a1.succeeded THEN a1.amt * pow(
            10,- a1.decimal
        )
        ELSE 0
    END AS from_amt,
    a2.mint AS to_mint,
    CASE
        WHEN a1.succeeded THEN a2.amt * pow(
            10,- a2.decimal
        )
        ELSE 0
    END AS to_amt
FROM
    agg a1
    LEFT OUTER JOIN agg a2
    ON a1.tx_id = a2.tx_id
    AND a1.rn <> a2.rn
    AND a2.rn = a2.max_rn
WHERE
    a1.rn = 1
    AND to_amt IS NOT NULL
