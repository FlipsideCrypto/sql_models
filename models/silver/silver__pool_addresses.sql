{{ config(
    materialized = 'incremental',
    unique_key = 'address',
    incremental_strategy = 'merge'
) }}

WITH swaps AS(

    SELECT
        swap_program,
        dim_asset_id__swap_from,
        pool_address,
        dim_asset_id__swap_to,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('core__fact_swap') }}

{% if is_incremental() %}
WHERE
    _INSERTED_TIMESTAMP >= (
        SELECT
            MAX(
                _INSERTED_TIMESTAMP
            )
        FROM
            {{ this }}
    ) - INTERVAL '4 HOURS'
{% endif %}
),
pool_names AS(
    SELECT
        swap_program,
        pool_address,
        CASE
            WHEN A.asset_id = 0 THEN 'ALGO'
            ELSE A.asset_name
        END AS swap_from_asset_name,
        CASE
            WHEN b.asset_id = 0 THEN 'ALGO'
            ELSE b.asset_name
        END AS swap_to_asset_name,
        s._INSERTED_TIMESTAMP
    FROM
        swaps s
        JOIN {{ ref('core__dim_asset') }} A
        ON s.dim_asset_id__swap_from = A.dim_asset_id
        JOIN {{ ref('core__dim_asset') }}
        b
        ON s.dim_asset_id__swap_to = b.dim_asset_id qualify ROW_NUMBER() over (
            PARTITION BY pool_address
            ORDER BY
                A.created_at DESC,
                b.created_at DESC
        ) = 1
)
SELECT
    'algorand' AS blockchain,
    'flipside' AS creator,
    pool_address AS address,
    'dex' AS label_type,
    'pool' AS label_subtype,
    swap_program AS label,
    swap_program || ': ' || swap_from_asset_name || '-' || swap_to_asset_name || ' pool' AS address_name,
    _INSERTED_TIMESTAMP
FROM
    pool_names
