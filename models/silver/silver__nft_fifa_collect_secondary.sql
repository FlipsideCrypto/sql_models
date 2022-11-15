{{ config(
    materialized = 'incremental',
    unique_key = ['nft_asset_id','purchase_timestamp'],
    incremental_strategy = 'merge'
) }}

WITH base AS (

    SELECT
        nft_asset_id,
        block_id,
        DATA,
        _INSERTED_TIMESTAMP
    FROM
        {{ source(
            'bronze_api',
            'nft_fifa_collect_secondary'
        ) }}

{% if is_incremental() %}
WHERE
    _INSERTED_TIMESTAMP >= (
        SELECT
            MAX(
                _INSERTED_TIMESTAMP
            )
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    nft_asset_id,
    b.value :amount :: INT AS amount,
    b.value :date :: datetime AS purchase_timestamp,
    b.value :recipient :address :: STRING AS recipient_address,
    b.value :recipient :username :: STRING AS recipient_username,
    b.value :sender :address :: STRING AS sender_address,
    b.value :sender :username :: STRING AS sender_username,
    _inserted_timestamp
FROM
    base A,
    LATERAL FLATTEN (PARSE_JSON(A.data) :data) b
WHERE
    b.value :type = 'purchase' qualify(ROW_NUMBER() over(PARTITION BY nft_asset_id, purchase_timestamp
ORDER BY
    _inserted_timestamp DESC) = 1)
