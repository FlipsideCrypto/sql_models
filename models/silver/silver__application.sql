{{ config(
    materialized = 'incremental',
    unique_key = 'app_id',
    incremental_strategy = 'merge',
    cluster_by = ['_inserted_timestamp::DATE']
) }}

SELECT
    INDEX AS app_id,
    algorand_decode_hex_addr(
        creator :: text
    ) AS creator_address,
    deleted AS app_closed,
    closed_at AS closed_at,
    created_at AS created_at,
    params,
    _inserted_timestamp
FROM
    {{ ref('bronze__application') }}

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
