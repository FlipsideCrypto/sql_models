{{ config(
    materialized = 'incremental',
    unique_key = ['block_id','intra'],
    incremental_strategy = 'merge',
    cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH base AS (

    SELECT
        block_id,
        intra,
        tx_id,
        tx_group_id,
        asset_id,
        asset_amount AS amount,
        asset_receiver,
        sender,
        app_id,
        TRY_BASE64_DECODE_STRING(
            tx_message :txn :apaa [0] :: STRING
        ) AS arg1,
        tx_type,
        _inserted_timestamp
    FROM
        {{ ref('silver__transaction') }}
    WHERE
        tx_type IN(
            'appl',
            'axfer'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
),
app_calls AS (
    SELECT
        block_id,
        MIN(intra) intra,
        tx_group_id,
        tx_id,
        LISTAGG(
            DISTINCT arg1,
            '-'
        ) AS arg1
    FROM
        base
    WHERE
        tx_type = 'appl'
        AND tx_group_id IS NOT NULL
        AND app_id IN (
            770103640,
            770102986
        )
    GROUP BY
        block_id,
        tx_group_id,
        tx_id
),
mid AS (
    SELECT
        b.tx_group_id,
        b.block_id,
        b.intra,
        b.tx_id,
        A.asset_id,
        CASE
            WHEN b.arg1 LIKE '%swap%' THEN 'inbound'
            WHEN b.arg1 LIKE '%issue%' THEN 'inbound'
            WHEN b.arg1 LIKE '%withdraw%' THEN 'outbound'
        END AS direction,
        A.amount,
        sender,
        asset_receiver,
        A._inserted_timestamp
    FROM
        base A
        INNER JOIN app_calls b
        ON A.tx_group_id = b.tx_group_id
    WHERE
        A.tx_type = 'axfer'
)
SELECT
    block_id,
    intra,
    tx_id,
    asset_id,
    amount,
    CASE
        WHEN asset_receiver IN (
            'MUJD2WXDDZLTZCKE2SBZVSBEQ55HT5RJH4BC2WHFV4DLP37EQNWT44ITWE',
            'LEHOHH76T2EDG4AY7HIZVDX7V6V7JSVMCT7VGE3HTK23BEDIILEWQP6SAU'
        ) THEN sender
        ELSE asset_receiver
    END AS bridger_address,
    CASE
        WHEN sender IN (
            'MUJD2WXDDZLTZCKE2SBZVSBEQ55HT5RJH4BC2WHFV4DLP37EQNWT44ITWE',
            'LEHOHH76T2EDG4AY7HIZVDX7V6V7JSVMCT7VGE3HTK23BEDIILEWQP6SAU'
        ) THEN asset_receiver
        ELSE sender
    END AS bridge_address,
    direction,
    _inserted_timestamp
FROM
    mid
WHERE
    CASE
        WHEN direction = 'inbound'
        AND asset_receiver IN (
            'MUJD2WXDDZLTZCKE2SBZVSBEQ55HT5RJH4BC2WHFV4DLP37EQNWT44ITWE',
            'LEHOHH76T2EDG4AY7HIZVDX7V6V7JSVMCT7VGE3HTK23BEDIILEWQP6SAU'
        ) THEN 'false'
        WHEN direction = 'outbound'
        AND sender = 'MUJD2WXDDZLTZCKE2SBZVSBEQ55HT5RJH4BC2WHFV4DLP37EQNWT44ITWE' THEN 'false'
        ELSE TRUE
    END = TRUE
