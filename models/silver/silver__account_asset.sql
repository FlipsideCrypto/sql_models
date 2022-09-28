{{ config(
    materialized = 'incremental',
    unique_key = '_unique_key',
    incremental_strategy = 'merge',
    cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH base AS (

    SELECT
        addr :: text AS address_raw,
        assetid AS asset_id,
        amount :: NUMBER AS amount,
        closed_at,
        created_at,
        deleted AS asset_closed,
        frozen,
        _inserted_timestamp
    FROM
        {{ ref('bronze__account_asset') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    b.address,
    A.asset_id,
    A.amount,
    A.closed_at,
    A.created_at,
    A.asset_closed,
    A.frozen,
    A._inserted_timestamp,
    concat_ws(
        '-',
        address,
        asset_id
    ) AS _unique_key
FROM
    base A
    JOIN {{ ref('silver__account') }}
    b
    ON A.address_raw = b.address_raw
