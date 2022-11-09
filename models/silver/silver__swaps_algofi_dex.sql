{{ config(
    materialized = 'incremental',
    unique_key = '_unique_key',
    incremental_strategy = 'merge',
    cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH algofi_app_ids AS (

    SELECT
        DISTINCT tx_message :txn :apid :: NUMBER AS app_id
    FROM
        {{ ref('silver__transaction') }}
    WHERE
        (
            inner_tx = 'FALSE'
            AND tx_message :dt :itx [2] :txn :type :: STRING = 'acfg'
            AND tx_message :dt :itx [2] :txn :apar :an :: STRING LIKE 'AF-POOL-%'
            AND tx_message :dt :itx [2] :txn :apar :au :: STRING = 'https://algofi.org'
        )
        OR (
            inner_tx = 'FALSE'
            AND tx_message :dt :itx [1] :txn :type :: STRING = 'acfg'
            AND tx_message :dt :itx [1] :txn :apar :an :: STRING LIKE 'AF-POOL-%'
            AND tx_message :dt :itx [1] :txn :apar :au :: STRING = 'https://algofi.org'
        )
),
base_tx AS (
    SELECT
        *
    FROM
        {{ ref('silver__transaction') }}

{% if is_incremental() %}
WHERE
    _INSERTED_TIMESTAMP :: DATE >= (
        SELECT
            MAX(
                _INSERTED_TIMESTAMP
            )
        FROM
            {{ this }}
    ) :: DATE - INTERVAL '4 HOURS'
    OR tx_group_id IN (
        SELECT
            tx_group_id
        FROM
            {{ this }}
        WHERE
            swap_from_asset_id IS NULL
    )
{% endif %}
),
tx_app_call AS (
    SELECT
        *
    FROM
        base_tx
    WHERE
        tx_type = 'appl'
),
tx_pay AS (
    SELECT
        *
    FROM
        base_tx
    WHERE
        tx_type = 'pay'
),
tx_a_tfer AS (
    SELECT
        pt.*,
        A.asset_name,
        A.decimals
    FROM
        base_tx pt
        JOIN {{ ref('silver__asset') }} A
        ON pt.asset_id = A.asset_id
    WHERE
        tx_type = 'axfer'
),
algofi_app AS(
    SELECT
        act.block_id,
        act.intra,
        act.tx_group_id,
        act._INSERTED_TIMESTAMP,
        act.sender AS sender,
        act.sender AS swapper,
        act.app_id,
        act.fee,
        CASE
            WHEN act.tx_message :dt :itx [0] :txn :type :: STRING = 'axfer' THEN asset_name
            WHEN act.tx_message :dt :itx [0] :txn :type :: STRING = 'pay' THEN 'ALGO'
        END AS to_asset_name,
        CASE
            WHEN act.tx_message :dt :itx [0] :txn :type :: STRING = 'axfer' THEN act.tx_message :dt :itx [0] :txn :xaid :: NUMBER
            WHEN act.tx_message :dt :itx [0] :txn :type :: STRING = 'pay' THEN 0
        END AS swap_to_asset_id,
        CASE
            WHEN act.tx_message :dt :itx [0] :txn :type :: STRING = 'axfer'
            AND decimals > 0 THEN act.tx_message :dt :itx [0] :txn :aamt :: FLOAT / pow(
                10,
                decimals
            )
            WHEN act.tx_message :dt :itx [0] :txn :type :: STRING = 'axfer'
            AND decimals = 0 THEN act.tx_message :dt :itx [0] :txn :aamt :: FLOAT
            WHEN act.tx_message :dt :itx [0] :txn :type :: STRING = 'pay' THEN act.tx_message :dt :itx [0] :txn :amt :: FLOAT / pow(
                10,
                6
            )
        END AS swap_to_amount,
        algorand_decode_b64_addr(
            tx_message :dt :itx [0] :txn :snd :: STRING
        ) AS pool_address
    FROM
        tx_app_call act
        JOIN {{ ref('silver__asset') }}
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
        pt.amount / pow(
            10,
            6
        ) - ZEROIFNULL(
            ref.tx_message :dt :itx [0] :txn :amt / pow(
                10,
                6
            ) :: FLOAT
        ) AS swap_from_amount,
        0 AS from_asset_id
    FROM
        algofi_app pa
        JOIN tx_pay pt
        ON pa.tx_group_id = pt.tx_group_id
        AND pa.swapper = pt.sender
        AND pa.intra -1 = pt.intra
        JOIN tx_app_call ref
        ON pa.tx_group_id = ref.tx_group_id
        AND pa.intra + 2 = ref.intra
    WHERE
        pt.inner_tx = 'FALSE'
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
        pt.asset_name AS from_asset_name,
        CASE
            WHEN pt.decimals > 0 THEN pt.asset_amount / pow(
                10,
                pt.decimals
            ) - ZEROIFNULL(
                ref.tx_message :dt :itx [0] :txn :aamt / pow(
                    10,
                    pt.decimals
                )
            )
            WHEN pt.decimals = 0 THEN pt.asset_amount - ZEROIFNULL(
                ref.tx_message :dt :itx [0] :txn :aamt
            )
        END :: FLOAT AS swap_from_amount,
        pt.asset_id AS from_asset_id
    FROM
        algofi_app pa
        JOIN tx_a_tfer pt
        ON pa.tx_group_id = pt.tx_group_id
        AND pa.intra -1 = pt.intra
        AND pa.sender = pt.sender
        LEFT JOIN tx_app_call ref
        ON pa.tx_group_id = ref.tx_group_id
        AND pa.intra + 2 = ref.intra
    WHERE
        pt.inner_tx = 'FALSE'
        AND ref.inner_tx = 'FALSE'
        AND TRY_BASE64_DECODE_STRING(
            ref.tx_message :txn :apaa [0] :: STRING
        ) <> 'sfe'
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
        act.sender AS swapper,
        act.app_id,
        act.fee,
        CASE
            WHEN act.tx_message :dt :itx [0] :txn :type :: STRING = 'axfer' THEN asset_name
            WHEN act.tx_message :dt :itx [0] :txn :type :: STRING = 'pay' THEN 'ALGO'
        END AS to_asset_name,
        CASE
            WHEN act.tx_message :dt :itx [0] :txn :type :: STRING = 'axfer' THEN act.tx_message :dt :itx [0] :txn :xaid :: NUMBER
            WHEN act.tx_message :dt :itx [0] :txn :type :: STRING = 'pay' THEN 0
        END AS swap_to_asset_id,
        CASE
            WHEN act.tx_message :dt :itx [0] :txn :type :: STRING = 'axfer'
            AND decimals > 0 THEN act.tx_message :dt :itx [0] :txn :aamt :: FLOAT / pow(
                10,
                decimals
            )
            WHEN act.tx_message :dt :itx [0] :txn :type :: STRING = 'axfer'
            AND decimals = 0 THEN act.tx_message :dt :itx [0] :txn :aamt :: FLOAT
            WHEN act.tx_message :dt :itx [0] :txn :type :: STRING = 'pay' THEN act.tx_message :dt :itx [0] :txn :amt :: FLOAT / pow(
                10,
                6
            )
        END AS swap_to_amount,
        algorand_decode_b64_addr(
            tx_message :dt :itx [0] :txn :snd :: STRING
        ) AS pool_address
    FROM
        tx_app_call act
        JOIN {{ ref('silver__asset') }}
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
        amount :: FLOAT / pow(
            10,
            6
        ) AS swap_from_amount,
        0 AS from_asset_id
    FROM
        algofi_appsef pa
        JOIN tx_pay pt
        ON pa.tx_group_id = pt.tx_group_id
        AND pa.swapper = pt.sender
        AND pa.intra -1 = pt.intra
    WHERE
        pt.inner_tx = 'FALSE'
),
from_axfer_swapssef AS(
    SELECT
        pt.tx_group_id,
        pt.intra,
        pt.sender AS swapper,
        pt.asset_name AS from_asset_name,
        CASE
            WHEN pt.decimals > 0 THEN pt.asset_amount / pow(
                10,
                pt.decimals
            )
            ELSE pt.asset_amount
        END :: FLOAT AS swap_from_amount,
        pt.asset_id AS from_asset_id
    FROM
        algofi_appsef pa
        JOIN tx_a_tfer pt
        ON pa.tx_group_id = pt.tx_group_id
        AND pa.intra -1 = pt.intra
        AND pa.swapper = pt.sender
    WHERE
        pt.inner_tx = 'FALSE'
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
        JOIN from_swapssef fs
        ON pa.tx_group_id = fs.tx_group_id
        AND pa.intra -1 = fs.intra
)
SELECT
    DISTINCT block_id,
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
    DISTINCT block_id,
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
