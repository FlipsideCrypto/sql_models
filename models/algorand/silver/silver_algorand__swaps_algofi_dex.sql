{{ config(
    materialized = 'incremental',
    unique_key = '_unique_key',
    incremental_strategy = 'merge',
    tags = ['snowflake', 'algorand', 'transactions', 'algorand_swaps']
) }}

WITH algofi_app_ids AS (

    SELECT
        DISTINCT tx_message :txn :apid :: NUMBER AS app_id
    FROM
        {{ ref('silver_algorand__transactions') }}
    WHERE
        inner_tx = 'FALSE'
        AND tx_message :dt :itx [2] :txn :type :: STRING = 'acfg'
        AND tx_message :dt :itx [2] :txn :apar :an :: STRING LIKE 'AF-POOL-%'
        AND tx_message :dt :itx [2] :txn :apar :au :: STRING = 'https://algofi.org'
        AND block_timestamp > '2022-02-02'
),
algofi_app AS(
    SELECT
        act.block_id,
        act.intra,
        act.tx_group_id,
        act._INSERTED_TIMESTAMP,
        act.sender,
        act.block_timestamp,
        act.sender AS swapper,
        act.app_id,
        act.fee,
        CASE
            WHEN act.tx_message :dt :itx [0] :txn :type :: STRING = 'axfer' THEN asa.asset_name
            WHEN act.tx_message :dt :itx [0] :txn :type :: STRING = 'pay' THEN 'ALGO'
        END AS to_asset_name,
        CASE
            WHEN act.tx_message :dt :itx [0] :txn :type :: STRING = 'axfer' THEN act.tx_message :dt :itx [0] :txn :xaid :: NUMBER
            WHEN act.tx_message :dt :itx [0] :txn :type :: STRING = 'pay' THEN 0
        END AS swap_to_asset_id,
        CASE
            WHEN act.tx_message :dt :itx [0] :txn :type :: STRING = 'axfer'
            AND asa.decimals > 0 THEN act.tx_message :dt :itx [0] :txn :aamt :: FLOAT / pow(
                10,
                asa.decimals
            )
            WHEN act.tx_message :dt :itx [0] :txn :type :: STRING = 'axfer'
            AND asa.decimals = 0 THEN act.tx_message :dt :itx [0] :txn :aamt :: FLOAT
            WHEN act.tx_message :dt :itx [0] :txn :type :: STRING = 'pay' THEN act.tx_message :dt :itx [0] :txn :amt :: FLOAT / pow(
                10,
                6
            )
        END AS swap_to_amount,
        algorand_decode_b64_addr(
            tx_message :dt :itx [0] :txn :snd :: STRING
        ) AS pool_address
    FROM
        {{ ref('silver_algorand__application_call_transaction') }}
        act
        LEFT JOIN {{ ref('silver_algorand__asset') }}
        asa
        ON act.tx_message :dt :itx [0] :txn :xaid :: NUMBER = asa.asset_id
    WHERE
        app_id IN (
            SELECT
                app_id
            FROM
                algofi_app_ids
        )
        AND TRY_BASE64_DECODE_STRING(
            act.tx_message :txn :apaa [0] :: STRING
        ) = 'sfe'
        AND inner_tx = 'FALSE'
),
from_pay_swapssfe AS(
    SELECT
        pa.tx_group_id AS tx_group_id,
        pt.intra,
        pt.sender AS swapper,
        'ALGO' AS from_asset_name,
        amount - ref.tx_message :dt :itx [0] :txn :amt / pow(
            10,
            6
        ) :: FLOAT AS swap_from_amount,
        0 AS from_asset_id
    FROM
        algofi_app pa
        LEFT JOIN {{ ref('silver_algorand__payment_transaction') }}
        pt
        ON pa.tx_group_id = pt.tx_group_id
        AND pa.swapper = pt.sender
        AND pa.intra -1 = pt.intra
        LEFT JOIN {{ ref('silver_algorand__application_call_transaction') }}
        ref
        ON pa.tx_group_id = ref.tx_group_id
    WHERE
        pt.inner_tx = 'FALSE'
        AND pt.tx_group_id IS NOT NULL
        AND ref.tx_group_id IS NOT NULL
        AND ref.inner_tx = 'FALSE'
        AND TRY_BASE64_DECODE_STRING(
            ref.tx_message :txn :apaa [0] :: STRING
        ) <> 'sfe'
),
from_axfer_swapssfe AS(
    SELECT
        pa.tx_group_id AS tx_group_id,
        pt.intra,
        pt.sender AS swapper,
        a2.asset_name AS from_asset_name,
        CASE
            WHEN a2.decimals > 0 THEN asset_amount / pow(
                10,
                A.decimals
            ) - ref.tx_message :dt :itx [0] :txn :aamt / pow(
                10,
                a2.decimals
            )
            WHEN a2.decimals = 0 THEN asset_amount - ref.tx_message :dt :itx [0] :txn :aamt
        END :: FLOAT AS swap_from_amount,
        pt.asset_id AS from_asset_id
    FROM
        algofi_app pa
        LEFT JOIN {{ ref('silver_algorand__asset_transfer_transaction') }}
        pt
        ON pa.tx_group_id = pt.tx_group_id
        AND pa.intra -1 = pt.intra
        LEFT JOIN {{ ref('silver_algorand__asset') }} A
        ON pt.asset_id = A.asset_id
        LEFT JOIN {{ ref('silver_algorand__application_call_transaction') }}
        ref
        ON pa.tx_group_id = ref.tx_group_id
        LEFT JOIN {{ ref('silver_algorand__asset') }}
        a2
        ON ref.tx_message :dt :itx [0] :txn :xaid :: NUMBER = a2.asset_id
    WHERE
        pt.inner_tx = 'FALSE'
        AND pt.tx_group_id IS NOT NULL
        AND ref.tx_group_id IS NOT NULL
        AND ref.inner_tx = 'FALSE'
        AND TRY_BASE64_DECODE_STRING(
            ref.tx_message :txn :apaa [0] :: STRING
        ) <> 'sfe'
        AND pa.sender = pt.sender
),
from_swapssfe AS(
    SELECT
        *
    FROM
        from_pay_swapssfe
    UNION
    SELECT
        *
    FROM
        from_axfer_swapssfe
),
allsfe AS(
    SELECT
        'sfe' AS TYPE,
        pa.block_id,
        pa.intra,
        pa.tx_group_id,
        pa._INSERTED_TIMESTAMP,
        block_timestamp,
        pa.swapper,
        app_id,
        fee,
        to_asset_name,
        swap_to_asset_id,
        pool_address,
        swap_to_amount,
        from_asset_name,
        from_asset_id,
        swap_from_amount
    FROM
        algofi_app pa
        LEFT JOIN from_swapssfe fs
        ON pa.tx_group_id = fs.tx_group_id
        AND pa.intra -1 = fs.intra
),
algofi_appsef AS(
    SELECT
        act.block_id,
        act.intra,
        act.tx_group_id,
        act._INSERTED_TIMESTAMP,
        act.block_timestamp,
        act.sender AS swapper,
        act.app_id,
        act.fee,
        CASE
            WHEN act.tx_message :dt :itx [0] :txn :type :: STRING = 'axfer' THEN asa.asset_name
            WHEN act.tx_message :dt :itx [0] :txn :type :: STRING = 'pay' THEN 'ALGO'
        END AS to_asset_name,
        CASE
            WHEN act.tx_message :dt :itx [0] :txn :type :: STRING = 'axfer' THEN act.tx_message :dt :itx [0] :txn :xaid :: NUMBER
            WHEN act.tx_message :dt :itx [0] :txn :type :: STRING = 'pay' THEN 0
        END AS swap_to_asset_id,
        CASE
            WHEN act.tx_message :dt :itx [0] :txn :type :: STRING = 'axfer'
            AND asa.decimals > 0 THEN act.tx_message :dt :itx [0] :txn :aamt :: FLOAT / pow(
                10,
                asa.decimals
            )
            WHEN act.tx_message :dt :itx [0] :txn :type :: STRING = 'axfer'
            AND asa.decimals = 0 THEN act.tx_message :dt :itx [0] :txn :aamt :: FLOAT
            WHEN act.tx_message :dt :itx [0] :txn :type :: STRING = 'pay' THEN act.tx_message :dt :itx [0] :txn :amt :: FLOAT / pow(
                10,
                6
            )
        END AS swap_to_amount,
        algorand_decode_b64_addr(
            tx_message :dt :itx [0] :txn :snd :: STRING
        ) AS pool_address
    FROM
        {{ ref('silver_algorand__application_call_transaction') }}
        act
        LEFT JOIN {{ ref('silver_algorand__asset') }}
        asa
        ON act.tx_message :dt :itx [0] :txn :xaid :: NUMBER = asa.asset_id
    WHERE
        app_id IN (
            SELECT
                app_id
            FROM
                algofi_app_ids
        )
        AND TRY_BASE64_DECODE_STRING(
            act.tx_message :txn :apaa [0] :: STRING
        ) = 'sef'
        AND inner_tx = 'FALSE'
),
from_pay_swapssef AS(
    SELECT
        pt.tx_group_id AS tx_group_id,
        pt.intra,
        pt.sender AS swapper,
        'ALGO' AS from_asset_name,
        amount AS swap_from_amount,
        0 AS from_asset_id
    FROM
        algofi_appsef pa
        LEFT JOIN {{ ref('silver_algorand__payment_transaction') }}
        pt
        ON pa.tx_group_id = pt.tx_group_id
        AND pa.swapper = pt.sender
        AND pa.intra -1 = pt.intra
    WHERE
        pt.inner_tx = 'FALSE'
        AND pt.tx_group_id IS NOT NULL
),
from_axfer_swapssef AS(
    SELECT
        pt.tx_group_id AS tx_group_id,
        pt.intra,
        pt.sender AS swapper,
        A.asset_name AS from_asset_name,
        CASE
            WHEN A.decimals > 0 THEN asset_amount / pow(
                10,
                A.decimals
            )
            ELSE asset_amount
        END :: FLOAT AS swap_from_amount,
        pt.asset_id AS from_asset_id
    FROM
        algofi_appsef pa
        LEFT JOIN {{ ref('silver_algorand__asset_transfer_transaction') }}
        pt
        ON pa.tx_group_id = pt.tx_group_id
        AND pa.intra -1 = pt.intra
        AND pa.swapper = pt.sender
        LEFT JOIN {{ ref('silver_algorand__asset') }} A
        ON pt.asset_id = A.asset_id
    WHERE
        pt.inner_tx = 'FALSE'
        AND pt.tx_group_id IS NOT NULL
),
from_swapssef AS(
    SELECT
        tx_group_id,
        intra,
        swapper,
        from_asset_name,
        swap_from_amount,
        from_asset_id
    FROM
        from_pay_swapssef
    UNION
    SELECT
        tx_group_id,
        intra,
        swapper,
        from_asset_name,
        swap_from_amount,
        from_asset_id
    FROM
        from_axfer_swapssef
),
allsef AS(
    SELECT
        'sef' AS TYPE,
        pa.block_id,
        pa.intra,
        pa.tx_group_id,
        pa._INSERTED_TIMESTAMP,
        block_timestamp,
        pa.swapper,
        app_id,
        fee,
        pool_address,
        to_asset_name,
        swap_to_asset_id,
        swap_to_amount,
        from_asset_name,
        from_asset_id,
        swap_from_amount
    FROM
        algofi_appsef pa
        LEFT JOIN from_swapssef fs
        ON pa.tx_group_id = fs.tx_group_id
        AND pa.intra -1 = fs.intra
    WHERE
        fs.tx_group_id IS NOT NULL
)
SELECT
    block_timestamp,
    block_id,
    intra,
    tx_group_id,
    app_id,
    swapper,
    from_asset_id AS swap_from_asset_id,
    swap_from_amount :: FLOAT AS swap_from_amount,
    pool_address,
    swap_to_asset_id,
    swap_to_amount :: FLOAT AS swap_to_amount,
    concat_ws(
        '-',
        block_id :: STRING,
        intra :: STRING
    ) AS _unique_key,
    _INSERTED_TIMESTAMP
FROM
    allsef
UNION
SELECT
    block_timestamp,
    block_id,
    intra,
    tx_group_id,
    app_id,
    swapper,
    from_asset_id AS swap_from_asset_id,
    swap_from_amount :: FLOAT AS swap_from_amount,
    pool_address,
    swap_to_asset_id,
    swap_to_amount :: FLOAT AS swap_to_amount,
    concat_ws(
        '-',
        block_id :: STRING,
        intra :: STRING
    ) AS _unique_key,
    _INSERTED_TIMESTAMP
FROM
    allsfe
WHERE
    1 = 1

{% if is_incremental() %}
AND _INSERTED_TIMESTAMP >= (
    SELECT
        MAX(
            _INSERTED_TIMESTAMP
        )
    FROM
        {{ this }}
)
OR block_timestamp IS NULL
{% endif %}
