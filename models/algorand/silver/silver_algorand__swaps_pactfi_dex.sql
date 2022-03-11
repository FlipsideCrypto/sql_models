{{ config(
    materialized = 'incremental',
    unique_key = '_unique_key',
    incremental_strategy = 'merge',
    tags = ['snowflake', 'algorand', 'transactions', 'algorand_swaps']
) }}

WITH pact_app_ids AS (

    SELECT
        DISTINCT tx_message :txn :apid :: NUMBER AS app_id
    FROM
        {{ ref('silver_algorand__transactions') }}
    WHERE
        inner_tx = 'FALSE'
        AND tx_message :dt :itx [0] :txn :type :: STRING = 'acfg'
        AND tx_message :dt :itx [0] :txn :apar :an :: STRING LIKE '%PACT LP Token'
        AND tx_message :dt :itx [0] :txn :apar :au :: STRING = 'https://pact.fi/'
        AND block_timestamp > '2022-02-02'
),
pactfi_app AS(
    SELECT
        act.block_id,
        act.intra,
        act.tx_group_id,
        block_timestamp,
        sender AS swapper,
        app_id,
        CASE
            WHEN tx_message :dt :itx [0] :txn :type :: STRING = 'axfer' THEN asa.asset_name :: STRING
            WHEN tx_message :dt :itx [0] :txn :type :: STRING = 'pay' THEN 'ALGO'
        END AS to_asset_name,
        CASE
            WHEN tx_message :dt :itx [0] :txn :type :: STRING = 'axfer' THEN tx_message :dt :itx [0] :txn :xaid :: NUMBER
            WHEN tx_message :dt :itx [0] :txn :type :: STRING = 'pay' THEN 0
        END AS to_asset_id,
        CASE
            WHEN tx_message :dt :itx [0] :txn :type :: STRING = 'axfer'
            AND asa.decimals > 0 THEN tx_message :dt :itx [0] :txn :aamt :: NUMBER / pow(
                10,
                asa.decimals
            )
            WHEN tx_message :dt :itx [0] :txn :type :: STRING = 'axfer'
            AND asa.decimals = 0 THEN tx_message :dt :itx [0] :txn :aamt :: NUMBER
            WHEN tx_message :dt :itx [0] :txn :type :: STRING = 'pay' THEN tx_message :dt :itx [0] :txn :amt :: NUMBER / pow(
                10,
                6
            )
        END AS swap_to_amount,
        algorand_decode_b64_addr(
            tx_message :dt :itx [0] :txn :snd :: STRING
        ) AS pool_address --:dt:itx:txn:type:xaid::number
    FROM
        {{ ref('silver_algorand__application_call_transaction') }}
        act
        LEFT JOIN {{ ref('silver_algorand__asset') }}
        asa
        ON act.tx_message :dt :itx [0] :txn :xaid :: NUMBER = asa.asset_id
    WHERE
        block_timestamp :: DATE > '2022-02-01'
        AND app_id IN (
            SELECT
                app_id
            FROM
                pact_app_ids
        )
        AND TRY_BASE64_DECODE_STRING(
            tx_message :txn :apaa [0] :: STRING
        ) = 'SWAP'
        AND inner_tx = 'FALSE'
),
from_pay_swaps AS(
    SELECT
        pa.tx_group_id AS tx_group_id,
        pt.intra,
        pt.sender AS swapper,
        'ALGO' AS from_asset_name,
        amount AS swap_from_amount,
        0 AS from_asset_id
    FROM
        pactfi_app pa
        LEFT JOIN {{ ref('silver_algorand__payment_transaction') }}
        pt
        ON pa.tx_group_id = pt.tx_group_id
        AND pa.swapper = pt.sender
        AND pa.intra -1 = pt.intra
    WHERE
        pt.inner_tx = 'FALSE'
        AND pt.tx_group_id IS NOT NULL
),
from_axfer_swaps AS(
    SELECT
        pa.tx_group_id AS tx_group_id,
        pt.intra,
        pt.sender AS swapper,
        A.asset_name AS from_asset_name,
        CASE
            WHEN decimals > 0 THEN asset_amount / pow(
                10,
                decimals
            )
            ELSE asset_amount
        END AS from_amount,
        pt.asset_id AS from_asset_id
    FROM
        pactfi_app pa
        LEFT JOIN {{ ref('silver_algorand__asset_transfer_transaction') }}
        pt
        ON pa.tx_group_id = pt.tx_group_id
        AND pa.intra -1 = pt.intra
        LEFT JOIN {{ ref('silver_algorand__asset') }} A
        ON pt.asset_id = A.asset_id
    WHERE
        pt.inner_tx = 'FALSE'
        AND pt.tx_group_id IS NOT NULL
        AND pa.swapper = pt.sender
),
from_swaps AS(
    SELECT
        *
    FROM
        from_pay_swaps
    UNION
    SELECT
        *
    FROM
        from_axfer_swaps
)
SELECT
    pa.block_timestamp,
    pa.block_id AS block_id,
    pa.intra AS intra,
    pa.tx_group_id AS tx_group_id,
    pa.app_id,
    fs.swapper,
    fs.from_asset_id AS swap_from_asset_id,
    fs.swap_from_amount,
    pa.pool_address AS pool_address,
    pa.to_asset_id AS swap_to_asset_id,
    pa.swap_to_amount,
    concat_ws(
        '-',
        pa.block_id :: STRING,
        pa.intra :: STRING
    ) AS _unique_key
FROM
    pactfi_app pa
    LEFT JOIN from_swaps fs
    ON pa.tx_group_id = fs.tx_group_id
    AND pa.intra -1 = fs.intra
