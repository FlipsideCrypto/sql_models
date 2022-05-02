{{ config(
    materialized = 'incremental',
    unique_key = 'address',
    incremental_strategy = 'merge',
    tags = ['snowflake', 'algorand', 'transactions', 'algorand_labels']
) }}

WITH swaps AS(

    SELECT
        'tinyman' AS swap_program,
        block_timestamp,
        block_id,
        intra,
        tx_group_id,
        app_id,
        swapper,
        swap_from_asset_id,
        swap_from_amount,
        pool_address,
        swap_to_asset_id,
        swap_to_amount,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver_algorand__swaps_tinyman_dex') }}
    UNION
    SELECT
        'algofi' AS swap_program,
        block_timestamp,
        block_id,
        intra,
        tx_group_id,
        app_id,
        swapper,
        swap_from_asset_id,
        swap_from_amount,
        pool_address,
        swap_to_asset_id,
        swap_to_amount,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver_algorand__swaps_algofi_dex') }}
    UNION
    SELECT
        'pactfi' AS swap_program,
        block_timestamp,
        block_id,
        intra,
        tx_group_id,
        app_id,
        swapper,
        swap_from_asset_id,
        swap_from_amount,
        pool_address,
        swap_to_asset_id,
        swap_to_amount,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver_algorand__swaps_pactfi_dex') }}
    UNION
    SELECT
        'wagmiswap' AS swap_program,
        block_timestamp,
        block_id,
        intra,
        tx_group_id,
        app_id,
        swapper,
        swap_from_asset_id,
        swap_from_amount,
        pool_address,
        swap_to_asset_id,
        swap_to_amount,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver_algorand__swaps_wagmiswap_dex') }}
),
pool_names AS(
    SELECT
        swap_program,
        pool_address,
        CASE
            WHEN swap_from_asset_id = 0 THEN 'ALGO'
            ELSE A.asset_name
        END AS swap_from_asset_name,
        CASE
            WHEN swap_to_asset_id = 0 THEN 'ALGO'
            ELSE b.asset_name
        END AS swap_to_asset_name,
        MAX(
            s._INSERTED_TIMESTAMP
        ) AS _INSERTED_TIMESTAMP
    FROM
        swaps s
        LEFT JOIN {{ ref('silver_algorand__asset') }} A
        ON s.swap_from_asset_id = A.asset_id
        LEFT JOIN {{ ref('silver_algorand__asset') }}
        b
        ON s.swap_to_asset_id = b.asset_id
    WHERE
        swap_to_asset_name IS NOT NULL
        AND swap_from_asset_name IS NOT NULL
    GROUP BY
        swap_program,
        pool_address,
        swap_from_asset_name,
        swap_to_asset_name qualify ROW_NUMBER() over (
            PARTITION BY pool_address
            ORDER BY
                swap_to_asset_name ASC,
                swap_from_asset_name ASC
        ) = 1
    ORDER BY
        swap_to_asset_name ASC,
        swap_from_asset_name ASC
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
WHERE
    1 = 1

{% if is_incremental() %}
AND _INSERTED_TIMESTAMP >= (
    SELECT
        MAX(
            _INSERTED_TIMESTAMP
        )
    FROM
        {{ this }}
) - INTERVAL '4 HOURS'
{% endif %}
