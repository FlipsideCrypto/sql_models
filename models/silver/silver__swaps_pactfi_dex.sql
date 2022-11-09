{{ config(
    materialized = 'incremental',
    unique_key = '_unique_key',
    incremental_strategy = 'merge'
) }}

WITH pact_app_ids AS (

    SELECT
        DISTINCT tx_message :txn :apid :: NUMBER AS app_id
    FROM
        {{ ref('silver__transaction') }}
    WHERE
        inner_tx = 'FALSE'
        AND tx_message :dt :itx [0] :txn :type :: STRING = 'acfg'
        AND tx_message :dt :itx [0] :txn :apar :an :: STRING LIKE '%PACT LP Token'
        AND tx_message :dt :itx [0] :txn :apar :au :: STRING = 'https://pact.fi/'
),
tx_base AS (
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
        tx_base
    WHERE
        tx_type = 'appl'
),
tx_pay AS (
    SELECT
        *
    FROM
        tx_base
    WHERE
        tx_type = 'pay'
),
tx_a_tfer AS (
    SELECT
        pt.*,
        A.asset_name,
        A.decimals
    FROM
        tx_base pt
        JOIN {{ ref('silver__asset') }} A
        ON pt.asset_id = A.asset_id
    WHERE
        tx_type = 'axfer'
),
pactfi_app AS(
    SELECT
        act.block_id,
        act.intra,
        act.tx_group_id,
        act._INSERTED_TIMESTAMP,
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
            AND asa.decimals > 0 THEN tx_message :dt :itx [0] :txn :aamt :: FLOAT / pow(
                10,
                asa.decimals
            )
            WHEN tx_message :dt :itx [0] :txn :type :: STRING = 'axfer'
            AND asa.decimals = 0 THEN tx_message :dt :itx [0] :txn :aamt :: FLOAT
            WHEN tx_message :dt :itx [0] :txn :type :: STRING = 'pay' THEN tx_message :dt :itx [0] :txn :amt :: FLOAT / pow(
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
        block_id > 18993713
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
        amount :: FLOAT / pow(
            10,
            6
        ) AS swap_from_amount,
        0 AS from_asset_id
    FROM
        pactfi_app pa
        JOIN tx_pay pt
        ON pa.tx_group_id = pt.tx_group_id
        AND pa.swapper = pt.sender
        AND pa.intra -1 = pt.intra
    WHERE
        pt.inner_tx = 'FALSE'
),
from_axfer_swaps AS(
    SELECT
        pa.tx_group_id AS tx_group_id,
        pt.intra,
        pt.sender AS swapper,
        asset_name AS from_asset_name,
        CASE
            WHEN decimals > 0 THEN asset_amount :: FLOAT / pow(
                10,
                decimals
            )
            ELSE asset_amount :: FLOAT
        END AS from_amount,
        asset_id AS from_asset_id
    FROM
        pactfi_app pa
        JOIN tx_a_tfer pt
        ON pa.tx_group_id = pt.tx_group_id
        AND pa.intra -1 = pt.intra
        AND pa.swapper = pt.sender
    WHERE
        pt.inner_tx = 'FALSE'
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
    pa.block_id,
    pa.intra,
    pa.tx_group_id,
    pa.app_id,
    fs.swapper,
    fs.from_asset_id AS swap_from_asset_id,
    ZEROIFNULL(
        fs.swap_from_amount
    ) :: FLOAT AS swap_from_amount,
    pa.pool_address AS pool_address,
    pa.to_asset_id AS swap_to_asset_id,
    ZEROIFNULL(
        pa.swap_to_amount
    ) :: FLOAT AS swap_to_amount,
    concat_ws(
        '-',
        pa.block_id :: STRING,
        pa.intra :: STRING
    ) AS _unique_key,
    pa._INSERTED_TIMESTAMP
FROM
    pactfi_app pa
    LEFT JOIN from_swaps fs
    ON pa.tx_group_id = fs.tx_group_id
    AND pa.intra -1 = fs.intra
