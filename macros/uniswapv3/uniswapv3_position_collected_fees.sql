{% macro uniswapv3_position_collected_fees(
        src_pools_table,
        src_events_emitted_table,
        src_positions_table,
        src_prices_table,
        src_transactions_table
    ) -%}
    WITH pools AS (
        SELECT
            pool_address,
            pool_name,
            token0_symbol,
            token1_symbol,
            token0_decimals,
            token1_decimals,
            token0,
            token1
        FROM
            {{ src_pools_table }}
            p
    ),
    pool_txs AS (
        SELECT
            tx_id,
            ee.contract_address AS pool_address,
            ee.event_index
        FROM
            {{ src_events_emitted_table }} ee
            INNER JOIN pools p
            ON p.pool_address = ee.contract_address
        WHERE
            event_name = 'Collect'
    ),
    burns AS (
        SELECT
            ee.tx_id,
            ee.event_inputs :amount0 :: STRING AS amount0_burned,
            ee.event_inputs :amount1 :: STRING AS amount1_burned,
            ee.event_inputs :tickLower :: STRING AS tick_lower,
            ee.event_inputs :tickUpper :: STRING AS tick_upper,
            ee.event_index
        FROM
            {{ src_events_emitted_table }}
            ee
        WHERE
            ee.tx_id IN (
                SELECT
                    tx_id
                FROM
                    pool_txs
            )
            AND event_name = 'Burn'
            AND (
                ee.event_inputs :amount0 > 0
                OR ee.event_inputs :amount1 > 0
            )
    ),
    -- Get nf position info by looking at corresponding nf pos event
    nf_positions AS (
        SELECT
            tx_id,
            contract_address AS nf_position_manager,
            event_inputs :tokenId AS token_id,
            REGEXP_REPLACE(
                ee.event_inputs :recipient,
                '\"',
                ''
            ) AS recipient,
            event_index
        FROM
            {{ src_events_emitted_table }}
            ee
        WHERE
            tx_id IN (
                SELECT
                    tx_id
                FROM
                    pool_txs
            )
            AND contract_address NOT IN (
                SELECT
                    pool_address
                FROM
                    pool_txs
            )
            AND event_inputs :tokenId IS NOT NULL
            AND event_name = 'Collect'
    ),
    all_lp_provider_txs AS (
        SELECT
            pool_address,
            nf_position_manager_address,
            liquidity_provider,
            nf_token_id,
            rn,
            tx_id
        FROM
            (
                SELECT
                    pool_address,
                    nf_position_manager_address,
                    liquidity_provider,
                    nf_token_id,
                    tx_id,
                    ROW_NUMBER() over (
                        PARTITION BY pool_address,
                        liquidity_provider,
                        nf_position_manager_address,
                        nf_token_id,
                        tx_id
                        ORDER BY
                            block_id DESC
                    ) AS rn
                FROM
                    {{ src_positions_table }}
                WHERE
                    pool_address IN (
                        SELECT
                            pool_address
                        FROM
                            pool_txs
                    )
                    AND nf_position_manager_address IN (
                        SELECT
                            nf_position_manager
                        FROM
                            nf_positions
                    )
                    AND nf_token_id IN (
                        SELECT
                            token_id
                        FROM
                            nf_positions
                    )
                    AND liquidity_provider IS NOT NULL
            ) sq
        WHERE
            rn = 1
    ),
    contract_lp_providers_token_ids AS (
        SELECT
            nf_token_id,
            COUNT(
                DISTINCT liquidity_provider
            )
        FROM
            all_lp_provider_txs
        GROUP BY
            1
        HAVING
            COUNT(1) > 1
    ),
    contract_lp_providers AS (
        SELECT
            alpt.pool_address,
            alpt.nf_position_manager_address,
            txs.to_address AS liquidity_provider,
            alpt.nf_token_id,
            alpt.tx_id
        FROM
            all_lp_provider_txs alpt
            LEFT JOIN {{ src_transactions_table }}
            txs
            ON alpt.tx_id = txs.tx_id
        WHERE
            nf_token_id IN (
                SELECT
                    DISTINCT nf_token_id
                FROM
                    contract_lp_providers_token_ids
            )
    ),
    regular_lp_providers AS (
        SELECT
            pool_address,
            nf_position_manager_address,
            liquidity_provider,
            nf_token_id,
            tx_id
        FROM
            all_lp_provider_txs
        WHERE
            nf_token_id NOT IN(
                SELECT
                    DISTINCT nf_token_id
                FROM
                    contract_lp_providers_token_ids
            )
    ),
    lp_providers AS (
        SELECT
            pool_address,
            nf_position_manager_address,
            liquidity_provider,
            nf_token_id,
            tx_id
        FROM
            regular_lp_providers
        UNION
        SELECT
            pool_address,
            nf_position_manager_address,
            liquidity_provider,
            nf_token_id,
            tx_id
        FROM
            contract_lp_providers
    ),
    prices AS (
        {{ safe_ethereum_prices(
            src_prices_table,
            '30 days',
            '9 months'
        ) }}
    ),
    FINAL AS (
        SELECT
            'ethereum' AS blockchain,
            ee.block_id,
            ee.block_timestamp,
            ee.tx_id,
            ee.event_index,
            contract_address AS pool_address,
            p.pool_name,
            COALESCE(
                lp_providers.liquidity_provider,
                REGEXP_REPLACE(
                    ee.event_inputs :recipient,
                    '\"',
                    ''
                )
            ) AS liquidity_provider,
            nf_positions.token_id :: INT AS nf_token_id,
            nf_positions.nf_position_manager AS nf_position_manager_address,
            p.token0_symbol,
            p.token1_symbol,
            CASE
                WHEN b.amount0_burned IS NOT NULL THEN {{ decimal_adjust_with_inputs(
                    "ee.event_inputs:amount0",
                    "(ee.event_inputs:amount0 - b.amount0_burned)",
                    "p.token0_decimals"
                ) }}
                ELSE {{ decimal_adjust(
                    "ee.event_inputs:amount0",
                    "p.token0_decimals"
                ) }}
            END AS amount0_adjusted,
            CASE
                WHEN b.amount1_burned IS NOT NULL THEN {{ decimal_adjust_with_inputs(
                    "ee.event_inputs:amount1",
                    "(ee.event_inputs:amount1 - b.amount1_burned)",
                    "p.token1_decimals"
                ) }}
                ELSE {{ decimal_adjust(
                    "ee.event_inputs:amount1",
                    "p.token1_decimals"
                ) }}
            END AS amount1_adjusted,
            CASE
                WHEN amount0_adjusted = 0 THEN 0
                ELSE {{ multiply(
                    "amount0_adjusted",
                    "prices_0.price"
                ) }}
            END AS amount0_usd,
            CASE
                WHEN amount1_adjusted = 0 THEN 0
                ELSE {{ multiply(
                    "amount1_adjusted",
                    "prices_1.price"
                ) }}
            END AS amount1_usd,
            event_inputs :tickLower :: INT AS tick_lower,
            event_inputs :tickUpper :: INT AS tick_upper,
            CASE
                WHEN event_inputs :tickLower IS NOT NULL
                AND p.token1_decimals IS NOT NULL
                AND p.token0_decimals IS NOT NULL THEN pow(
                    1.0001,
                    event_inputs :tickLower
                ) / pow(
                    10,
                    p.token1_decimals - p.token0_decimals
                )
                ELSE NULL
            END AS price_lower,
            CASE
                WHEN event_inputs :tickUpper IS NOT NULL
                AND p.token1_decimals IS NOT NULL
                AND p.token0_decimals IS NOT NULL THEN pow(
                    1.0001,
                    event_inputs :tickUpper
                ) / pow(
                    10,
                    p.token1_decimals - p.token0_decimals
                )
                ELSE NULL
            END AS price_upper,
            {{ multiply(
                "price_lower",
                "prices_1.price"
            ) }} AS price_lower_usd,
            {{ multiply(
                "price_upper",
                "prices_1.price"
            ) }} AS price_upper_usd
        FROM
            {{ src_events_emitted_table }}
            ee -- Limit collect events to uniswap v3 pools only
            INNER JOIN pools p
            ON p.pool_address = ee.contract_address -- get nf positions in this txs
            LEFT OUTER JOIN nf_positions
            ON ee.tx_id = nf_positions.tx_id
            AND ee.event_index = nf_positions.event_index -- get lp info in this txs
            LEFT OUTER JOIN lp_providers
            ON lp_providers.tx_id = nf_positions.tx_id
            AND lp_providers.nf_position_manager_address = nf_positions.nf_position_manager
            AND lp_providers.pool_address = ee.contract_address -- get pool burns in this tx
            LEFT OUTER JOIN burns b
            ON ee.tx_id = b.tx_id
            AND ee.event_inputs :tickLower = b.tick_lower
            AND ee.event_inputs :tickUpper = b.tick_upper -- get USD prices
            LEFT OUTER JOIN prices prices_0
            ON prices_0.hour = DATE_TRUNC(
                'day',
                ee.block_timestamp
            )
            AND p.token0 = prices_0.token_address
            LEFT OUTER JOIN prices prices_1
            ON prices_1.hour = DATE_TRUNC(
                'day',
                ee.block_timestamp
            )
            AND p.token1 = prices_1.token_address -- Filter to Pool Collect events
        WHERE
            event_name = 'Collect' -- We don't want collect events where nothing was collected. If there are multiple collect events
            -- in a tx, and there are burn events we want to ensure we're pairing up the burns against collect
            -- events with actual amounts. Otherwise we end up with negative collects.
            AND (
                ee.event_inputs :amount0 > 0
                OR ee.event_inputs :amount1 > 0
            )
            AND NF_TOKEN_ID is not NULL
        HAVING
            (
                amount0_adjusted >= 0
                OR amount1_adjusted >= 0
            )
    ),
    grouped_final AS (
        SELECT
            *
        FROM
            FINAL
        GROUP BY
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            16,
            17,
            18,
            19,
            20,
            21,
            22
    )
SELECT
    *
FROM
    grouped_final
{%- endmacro %}
