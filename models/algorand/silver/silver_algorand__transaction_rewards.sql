{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', block_id, intra, account)",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH base AS (

    SELECT
        tx_ID,
        DATA
    FROM
        {{ ref('silver_algorand__indexer_tx') }}

{% if is_incremental() %}
WHERE
    _PARTITION_BY_DATE >= CURRENT_DATE -2
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY tx_id
ORDER BY
    _PARTITION_BY_DATE DESC)) = 1
),
inner_outer AS (
    SELECT
        A.tx_ID,
        A.data :"confirmed-round" :: INT AS block_id,
        A.data :"intra-round-offset" :: INT AS intra,
        C.value :sender :: STRING AS sender,
        C.value :"sender-rewards" AS sender_rewards,
        C.value :"payment-transaction" :"receiver" :: STRING AS reciever,
        C.value :"receiver-rewards" AS reciever_rewards
    FROM
        base A,
        LATERAL FLATTEN(
            input => A.data :"inner-txns"
        ) C
    UNION ALL
    SELECT
        A.tx_id,
        A.data :"confirmed-round" :: INT AS block_id,
        A.data :"intra-round-offset" :: INT AS intra,
        A.data :sender :: STRING AS sender,
        A.data :"sender-rewards" AS sender_rewards,
        A.data :"payment-transaction" :"receiver" :: STRING AS reciever,
        A.data :"receiver-rewards" AS reciever_rewards
    FROM
        base A
)
SELECT
    b.block_timestamp,
    A.intra,
    A.block_id,
    A.tx_id,
    A.account,
    SUM(amount) amount
FROM
    (
        SELECT
            tx_id,
            block_id,
            intra,
            sender AS account,
            sender_rewards AS amount
        FROM
            inner_outer
        WHERE
            sender_rewards > 0
        UNION ALL
        SELECT
            tx_id,
            block_id,
            intra,
            reciever AS account,
            reciever_rewards AS amount
        FROM
            inner_outer
        WHERE
            reciever_rewards > 0
    ) A
    JOIN {{ ref('silver_algorand__block') }}
    b
    ON A.block_ID = b.block_ID
GROUP BY
    b.block_timestamp,
    A.intra,
    A.block_id,
    A.tx_id,
    A.account
