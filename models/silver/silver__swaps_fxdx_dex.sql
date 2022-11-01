{{ config(
    materialized = 'incremental',
    unique_key = '_unique_key',
    incremental_strategy = 'merge'
) }}

WITH base AS (

    SELECT
        sender,
        tx_group_id,
        tx_message,
        tx_type,
        app_id,
        intra,
        block_id,
        asset_id,
        COALESCE(
            receiver,
            asset_receiver
        ) AS receiver,
        COALESCE(
            asset_amount,
            amount
        ) AS amount,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__transaction') }}
    WHERE
        block_id >= 22226314

{% if is_incremental() %}
AND _INSERTED_TIMESTAMP >= (
    SELECT
        MAX(
            _INSERTED_TIMESTAMP
        )
    FROM
        {{ this }}
) - INTERVAL '24 HOURS'
{% endif %}
),
app_call_base AS (
    SELECT
        DISTINCT intra,
        block_id,
        sender,
        tx_group_id,
        app_id,
        f.value AS sub_app_id,
        _INSERTED_TIMESTAMP
    FROM
        base,
        TABLE(FLATTEN(tx_message: txn :apfa)) f
    WHERE
        tx_type = 'appl'
        AND app_id = '808867994'
        AND tx_message :txn :apaa [0] = 'c3dhcA==' --swap
        AND tx_group_id IS NOT NULL
),
subapps AS (
    SELECT
        DISTINCT A.tx_group_id,
        silver.algorand_decode_b64_addr(
            f.value
        ) pool_address,
        TRY_BASE64_DECODE_STRING(
            tx_message :txn :apaa [0]
        ) TYPE,
        tx_message :txn :apas [0] asset_id,
        b.sub_app_id,
        A.intra
    FROM
        base A,
        app_call_base b,
        TABLE(FLATTEN(tx_message: txn :apat)) f
    WHERE
        A.tx_group_id = b.tx_group_id
        AND A.app_id = b.sub_app_id
        AND tx_type = 'appl'
        AND b.sender <> silver.algorand_decode_b64_addr(
            f.value
        )
),
filtered AS (
    SELECT
        DISTINCT b.tx_group_id,
        A.asset_id,
        A.sender,
        A.receiver
    FROM
        base A
        JOIN subapps b
        ON A.tx_group_id = b.tx_group_id
        AND (
            (
                A.sender = b.pool_address
                AND b.type = 'swapOut'
            )
            OR (
                A.receiver = b.pool_address
                AND b.type = 'swapIn'
            )
        )
    WHERE
        A.tx_type IN (
            'pay',
            'axfer'
        )
),
all_rows AS (
    SELECT
        DISTINCT A.intra,
        b.tx_group_id,
        A.asset_id,
        A.sender,
        A.receiver,
        A.amount
    FROM
        base A
        JOIN (
            SELECT
                DISTINCT A.sender addy,
                A.tx_group_id
            FROM
                filtered A
                LEFT JOIN app_call_base b
                ON A.tx_group_id = b.tx_group_id
                AND A.sender = b.sender
            WHERE
                b.sender IS NULL
            UNION
            SELECT
                DISTINCT A.receiver,
                A.tx_group_id
            FROM
                filtered A
                LEFT JOIN app_call_base b
                ON A.tx_group_id = b.tx_group_id
                AND A.receiver = b.sender
            WHERE
                b.sender IS NULL
        ) b
        ON A.tx_group_id = b.tx_group_id
        AND (
            A.sender = b.addy
            OR A.receiver = b.addy
        )
    WHERE
        A.tx_type IN (
            'pay',
            'axfer'
        )
),
rankings AS (
    SELECT
        ROW_NUMBER() over(
            PARTITION BY tx_group_id
            ORDER BY
                intra
        ) swap_in_rank,
        ROW_NUMBER() over(
            PARTITION BY tx_group_id
            ORDER BY
                intra DESC
        ) swap_out_rank,*
    FROM
        all_rows
),
fin AS (
    SELECT
        tx_group_id,
        OBJECT_AGG(
            swap_in_rank :: STRING,
            amount :: variant
        ) AS j_amount_in,
        OBJECT_AGG(
            swap_out_rank :: STRING,
            amount :: variant
        ) AS j_amount_out,
        OBJECT_AGG(
            swap_in_rank :: STRING,
            asset_id :: variant
        ) AS j_asset_in,
        OBJECT_AGG(
            swap_out_rank :: STRING,
            asset_id :: variant
        ) AS j_asset_out,
        OBJECT_AGG(
            swap_in_rank :: STRING,
            receiver :: variant
        ) AS j_RECEIVER_in,
        OBJECT_AGG(
            swap_out_rank :: STRING,
            sender :: variant
        ) AS j_sender_out,
        j_amount_in :"1" AS swap_from_amount,
        j_asset_in :"1" AS swap_from_asset_id,
        j_amount_out :"1" AS swap_to_amount,
        j_asset_out :"1" AS swap_to_asset_id,
        CASE
            WHEN j_amount_in :"1" = j_amount_in :"2" THEN j_RECEIVER_in :"2"
            WHEN j_amount_out :"1" = j_amount_out :"2" THEN j_sender_out :"2"
            ELSE j_sender_out :"1"
        END :: STRING AS pool_address,
        CASE
            WHEN j_amount_in :"1" = j_amount_in :"2"
            AND j_asset_in :"1" NOT IN (
                31566704,
                312769
            ) THEN j_RECEIVER_in :"1"
            WHEN j_amount_out :"1" = j_amount_out :"2"
            AND j_asset_out :"1" NOT IN (
                31566704,
                312769
            ) THEN j_sender_out :"1"
        END :: STRING AS wrapped_pool,
        CASE
            WHEN wrapped_pool IS NOT NULL
            AND j_sender_out :"1" <> j_RECEIVER_in :"1" THEN j_asset_in :"2"
        END AS wrapped_asset_id
    FROM
        rankings
    GROUP BY
        tx_group_id
)
SELECT
    b.block_id,
    b.intra,
    A.tx_group_id,
    b.app_id,
    b.sender AS swapper,
    A.swap_from_asset_id,
    A.swap_from_amount,
    A.pool_address,
    A.swap_to_asset_id,
    A.swap_to_amount,
    A.wrapped_asset_id,
    A.wrapped_pool,
    concat_ws(
        '-',
        b.block_id :: STRING,
        b.intra :: STRING
    ) AS _unique_key,
    b._inserted_timestamp
FROM
    fin A
    JOIN(
        SELECT
            DISTINCT block_id,
            intra,
            tx_group_id,
            app_id,
            sender,
            _inserted_timestamp
        FROM
            app_call_base
    ) b
    ON A.tx_group_id = b.tx_group_id
