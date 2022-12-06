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
        DISTINCT tx_group_id
    FROM
        base
    WHERE
        tx_type = 'appl'
        AND app_id = 896045597
)
SELECT
    A.block_id,
    A.intra,
    A.tx_id,
    A.asset_id,
    A.amount,
    CASE
        WHEN sender IN (
            'OGRVZ5KZCNZB4LRCOBLGEYXKLDVWI4C3PVMV6KTV4DOVW5ROK6Q5IQ3EZU',
            'MJCB6YDQQENASEPBAJYPAOZCQ6ZM67WJA226RJYZH6OTOVOUB72QEUSCXA',
            'HHANG4J6ESRY6X27N6V7TZLQ5WBTF67PEG3DFDAFJ7FZ5Q2JRGKWK3C34E'
        ) THEN asset_receiver
        WHEN asset_receiver IN (
            'OGRVZ5KZCNZB4LRCOBLGEYXKLDVWI4C3PVMV6KTV4DOVW5ROK6Q5IQ3EZU',
            'MJCB6YDQQENASEPBAJYPAOZCQ6ZM67WJA226RJYZH6OTOVOUB72QEUSCXA',
            'HHANG4J6ESRY6X27N6V7TZLQ5WBTF67PEG3DFDAFJ7FZ5Q2JRGKWK3C34E'
        ) THEN sender
    END AS bridger_address,
    CASE
        WHEN sender IN (
            'OGRVZ5KZCNZB4LRCOBLGEYXKLDVWI4C3PVMV6KTV4DOVW5ROK6Q5IQ3EZU',
            'MJCB6YDQQENASEPBAJYPAOZCQ6ZM67WJA226RJYZH6OTOVOUB72QEUSCXA',
            'HHANG4J6ESRY6X27N6V7TZLQ5WBTF67PEG3DFDAFJ7FZ5Q2JRGKWK3C34E'
        ) THEN sender
        WHEN asset_receiver IN (
            'OGRVZ5KZCNZB4LRCOBLGEYXKLDVWI4C3PVMV6KTV4DOVW5ROK6Q5IQ3EZU',
            'MJCB6YDQQENASEPBAJYPAOZCQ6ZM67WJA226RJYZH6OTOVOUB72QEUSCXA',
            'HHANG4J6ESRY6X27N6V7TZLQ5WBTF67PEG3DFDAFJ7FZ5Q2JRGKWK3C34E'
        ) THEN asset_receiver
    END AS bridge_address,
    CASE
        WHEN sender = bridge_address THEN 'inbound'
        WHEN asset_receiver = bridge_address THEN 'outbound'
    END direction,
    A._inserted_timestamp
FROM
    base A
    INNER JOIN app_calls b
    ON A.tx_group_id = b.tx_group_id
WHERE
    A.tx_type = 'axfer'
    AND amount IS NOT NULL
    AND asset_receiver <> '4QVCSC3DMV5622OPKJXGOQNOADCHGZDWXKC47DN5WDZHRP4SW72GRSDALE'
    AND sender <> '4QVCSC3DMV5622OPKJXGOQNOADCHGZDWXKC47DN5WDZHRP4SW72GRSDALE'
