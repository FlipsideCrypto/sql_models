{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', block_id, tx_id, event_id)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp', 'block_id'],
    tags = ['snowflake', 'polygon', 'polygon_udm_events_gold', 'address_labels']
) }}

WITH token_prices AS (

    SELECT
        p.symbol,
        DATE_TRUNC(
            'hour',
            recorded_at
        ) AS HOUR,
        LOWER(
            A.token_address
        ) AS token_address,
        AVG(price) AS price
    FROM
        {{ source(
            'shared',
            'prices_v2'
        ) }}
        p
        JOIN {{ source(
            'shared',
            'market_asset_metadata'
        ) }} A
        ON p.asset_id :: STRING = A.asset_id :: STRING
    WHERE
        A.platform IN (
            'polygon',
            'polygon-pos'
        )

{% if is_incremental() %}
AND recorded_at :: DATE >= (
    SELECT
        MAX(
            block_timestamp :: DATE
        )
    FROM
        {{ this }}
)
{% endif %}
GROUP BY
    p.symbol,
    HOUR,
    token_address
),
poly_prices AS (
    SELECT
        p.symbol,
        DATE_TRUNC(
            'hour',
            recorded_at
        ) AS HOUR,
        AVG(price) AS price
    FROM
        {{ source(
            'shared',
            'prices_v2'
        ) }}
        p
    WHERE
        p.asset_id = '3890'

{% if is_incremental() %}
AND recorded_at >= (
    SELECT
        MAX(
            block_timestamp :: DATE
        )
    FROM
        {{ this }}
)
{% endif %}
GROUP BY
    1,
    2
),
poly_labels AS (
    SELECT
        l1_label,
        l2_label,
        project_name,
        address_name,
        address
    FROM
        {{ ref('silver_crosschain__address_labels') }}
    WHERE
        blockchain = 'polygon'
),
base_udm_events AS (
    SELECT
        *
    FROM
        {{ ref('silver_polygon__udm_events') }}
    WHERE
        1 = 1

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
    SELECT
        MAX(
            block_timestamp :: DATE
        )
    FROM
        {{ this }}
)
{% endif %}
),
base_tx AS (
    SELECT
        *
    FROM
        {{ ref('silver_polygon__transactions') }}
    WHERE
        1 = 1

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
    SELECT
        MAX(
            block_timestamp :: DATE
        )
    FROM
        {{ this }}
)
{% endif %}
),
events AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        from_address,
        from_labels.l1_label AS from_label_type,
        from_labels.l2_label AS from_label_subtype,
        from_labels.project_name AS from_label,
        from_labels.address_name AS from_address_name,
        to_address,
        to_labels.l1_label AS to_label_type,
        to_labels.l2_label AS to_label_subtype,
        to_labels.project_name AS to_label,
        to_labels.address_name AS to_address_name,
        log_method AS event_name,
        NULL AS event_type,
        log_index AS event_id,
        contract_address,
        CASE
            WHEN e.symbol = 'ETH' THEN 'MATIC'
            ELSE e.symbol
        END AS symbol,
        input_method,
        native_value,
        token_value,
        fee
    FROM
        base_udm_events e
        LEFT OUTER JOIN poly_labels AS from_labels
        ON e.from_address = from_labels.address
        LEFT OUTER JOIN poly_labels AS to_labels
        ON e.to_address = to_labels.address
        LEFT OUTER JOIN poly_labels AS contract_labels
        ON e.contract_address = contract_labels.address
),
originator AS (
    SELECT
        tx_id,
        t.from_address AS origin_address,
        from_labels.l1_label AS origin_label_type,
        from_labels.l2_label AS origin_label_subtype,
        from_labels.project_name AS origin_label,
        from_labels.address_name AS origin_address_name,
        t.input_method AS origin_function_signature,
        f.text_signature AS origin_function_name
    FROM
        base_tx t
        LEFT OUTER JOIN {{ source(
            'ethereum',
            'sha256_function_signatures'
        ) }} AS f
        ON t.input_method = f.hex_signature
        AND f.importance = 1
        LEFT OUTER JOIN poly_labels AS from_labels
        ON t.from_address = from_labels.address
)
SELECT
    block_timestamp,
    block_id,
    e.tx_id,
    o.origin_address,
    o.origin_label_type,
    o.origin_label_subtype,
    o.origin_label,
    o.origin_address_name,
    o.origin_function_signature,
    o.origin_function_name,
    CASE
        WHEN event_name IS NOT NULL
        AND event_name != '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' THEN NULL :: STRING
        ELSE from_address
    END AS from_address,
    CASE
        WHEN event_name IS NOT NULL
        AND event_name != '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' THEN NULL :: STRING
        ELSE from_label_type
    END AS from_label_type,
    CASE
        WHEN event_name IS NOT NULL
        AND event_name != '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' THEN NULL :: STRING
        ELSE from_label_subtype
    END AS from_label_subtype,
    CASE
        WHEN event_name IS NOT NULL
        AND event_name != '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' THEN NULL :: STRING
        ELSE from_label
    END AS from_label,
    CASE
        WHEN event_name IS NOT NULL
        AND event_name != '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' THEN NULL :: STRING
        ELSE from_address_name
    END AS from_address_name,
    CASE
        WHEN event_name IS NOT NULL
        AND event_name != '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' THEN NULL :: STRING
        ELSE to_address
    END AS to_address,
    CASE
        WHEN event_name IS NOT NULL
        AND event_name != '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' THEN NULL :: STRING
        ELSE to_label_type
    END AS to_label_type,
    CASE
        WHEN event_name IS NOT NULL
        AND event_name != '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' THEN NULL :: STRING
        ELSE to_label_subtype
    END AS to_label_subtype,
    CASE
        WHEN event_name IS NOT NULL
        AND event_name != '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' THEN NULL :: STRING
        ELSE to_label
    END AS to_label,
    CASE
        WHEN event_name IS NOT NULL
        AND event_name != '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' THEN NULL :: STRING
        ELSE to_address_name
    END AS to_address_name,
    CASE
        WHEN e.event_name = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        OR token_value > 0 THEN 'transfer'
        WHEN event_name IS NOT NULL
        AND event_name != '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' THEN COALESCE(
            d.method,
            event_name
        )
        ELSE event_name
    END AS event_name,
    CASE
        WHEN e.event_name = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' THEN 'erc20_transfer'
        WHEN native_value > 0 THEN 'native_matic'
        WHEN event_name IS NOT NULL
        AND event_name != '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' THEN d.type
        ELSE event_type
    END AS event_type,
    event_id,
    contract_address,
    CASE
        WHEN e.event_name = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' THEN COALESCE(
            e.symbol,
            p.symbol
        )
        WHEN native_value > 0 THEN 'MATIC'
        ELSE e.symbol
    END AS symbol,
    -- native_value,
    CASE
        WHEN event_name IS NOT NULL
        AND event_name != '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' THEN NULL :: FLOAT
        WHEN native_value > 0 THEN native_value
        ELSE token_value / pow(
            10,
            de.decimal
        )
    END amount,
    CASE
        WHEN event_name IS NOT NULL
        AND event_name != '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' THEN NULL :: FLOAT
        WHEN e.event_name = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' THEN token_value * p.price / pow(
            10,
            de.decimal
        )
        WHEN native_value > 0 THEN native_value * pp.price
        ELSE NULL
    END AS amount_usd
FROM
    events e
    JOIN originator o
    ON e.tx_id = o.tx_id
    LEFT JOIN token_prices p
    ON p.token_address = e.contract_address
    AND DATE_TRUNC(
        'hour',
        e.block_timestamp
    ) = p.hour
    LEFT JOIN poly_prices pp
    ON DATE_TRUNC(
        'hour',
        e.block_timestamp
    ) = pp.hour
    LEFT JOIN {{ source(
        'ethereum',
        'ethereum_decoded_log_methods'
    ) }}
    d
    ON e.event_name = d.encoded_log_method
    LEFT JOIN {{ ref('polygon_dbt__decimals') }}
    de
    ON LOWER(
        de.token_id
    ) = LOWER(
        e.contract_address
    )
