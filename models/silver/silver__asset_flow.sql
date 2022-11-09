{{ config(
    materialized = 'incremental',
    unique_key = ['_unique_key','block_id'],
    incremental_strategy = 'merge',
    cluster_by = ['_inserted_timestamp::DATE', 'block_id'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(asset_id, address);"
) }}

WITH base AS (

    SELECT
        block_id,
        asset_id,
        COALESCE(
            asset_sender,
            sender
        ) address,
        -1 * asset_amount AS amount,
        _inserted_timestamp
    FROM
        {{ ref('silver__transaction') }}
    WHERE
        tx_type = 'axfer'
        AND asset_amount > 0

{% if is_incremental() %}
AND _inserted_timestamp >= (
    (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
)
{% endif %}
UNION ALL
SELECT
    block_id,
    asset_id,
    COALESCE(
        asset_receiver,
        receiver
    ) address,
    asset_amount AS amount,
    _inserted_timestamp
FROM
    {{ ref('silver__transaction') }}
WHERE
    tx_type = 'axfer'
    AND asset_amount > 0

{% if is_incremental() %}
AND _inserted_timestamp >= (
    (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
)
{% endif %}
UNION ALL
SELECT
    block_id,
    asset_id,
    account,
    amount,
    _inserted_timestamp
FROM
    {{ ref('silver__transaction_close') }}
WHERE
    amount > 0
    AND asset_id > 0

{% if is_incremental() %}
AND _inserted_timestamp >= (
    (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
)
{% endif %}
UNION ALL
SELECT
    A.block_id,
    A.asset_id,
    b.sender AS account,
    amount,
    _inserted_timestamp
FROM
    {{ ref('silver__transaction_close') }} A
    JOIN (
        SELECT
            DISTINCT block_id,
            intra,
            sender
        FROM
            {{ ref('silver__transaction') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        (
            SELECT
                MAX(
                    _inserted_timestamp
                )
            FROM
                {{ this }}
        )
    )
{% endif %}
) b
ON A.block_id = b.block_id
AND A.intra = b.intra
WHERE
    amount > 0
    AND asset_id > 0

{% if is_incremental() %}
AND _inserted_timestamp >= (
    (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
)
{% endif %}
)
SELECT
    block_id,
    A.asset_id,
    address,
    SUM(
        CASE
            WHEN decimals > 0 THEN amount / pow(
                10,
                decimals
            )
            ELSE amount
        END
    ) AS amount,
    concat_ws(
        '-',
        address,
        A.asset_id,
        block_id
    ) AS _unique_key,
    MIN(
        A._inserted_timestamp
    ) AS _inserted_timestamp
FROM
    base A
    LEFT JOIN {{ ref('silver__asset') }}
    b
    ON A.asset_id = b.asset_id
GROUP BY
    block_id,
    A.asset_id,
    address
