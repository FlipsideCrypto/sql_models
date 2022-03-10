{% macro uniswapv3_position_collected_fees(src_pools_table, src_events_emitted_table, src_positions_table, src_prices_table) -%}
    WITH pools as (
        SELECT 
            pool_address,
            pool_name,
            token0_symbol,
            token1_symbol,
            token0_decimals,
            token1_decimals,
            token0,
            token1
        FROM {{ src_pools_table }} p
        WHERE
        p.block_timestamp >= getdate() - interval '9 months'
    ),
    pool_txs as (
        SELECT 
            tx_id,
            ee.contract_address as pool_address
        FROM {{ src_events_emitted_table }} ee
        INNER JOIN pools p ON p.pool_address = ee.contract_address
        WHERE 
            event_name = 'Collect' 
            AND ee.block_timestamp >= getdate() - interval '9 months'
    ),
    burns as (
        SELECT
            ee.tx_id, 
            ee.event_inputs:amount0 as amount0_burned,
            ee.event_inputs:amount1 as amount1_burned,
            ee.event_inputs:tickLower as tick_lower,
            ee.event_inputs:tickUpper as tick_upper
        FROM {{ src_events_emitted_table }} ee
        WHERE 
            ee.tx_id IN (SELECT tx_id FROM pool_txs)
            AND event_name = 'Burn'
            AND (ee.event_inputs:amount0 > 0 or ee.event_inputs:amount1 > 0)
            AND ee.block_timestamp >= getdate() - interval '9 months'
    ),
    -- Get nf position info by looking at corresponding nf pos event
    nf_positions as (
        SELECT 
            tx_id,
            contract_address as nf_position_manager,
            event_inputs:tokenId as token_id,
            REGEXP_REPLACE(ee.event_inputs:recipient,'\"','') as recipient
        FROM {{ src_events_emitted_table }} ee
        WHERE 
            tx_id IN (SELECT tx_id FROM pool_txs) 
            AND contract_address NOT IN (SELECT pool_address FROM pool_txs)
            AND event_inputs:tokenId is not null 
            AND event_name = 'Collect'
            AND ee.block_timestamp >= getdate() - interval '9 months'
    ),
    lp_providers as (
        SELECT * FROM (
            SELECT 
                pool_address,
                NF_POSITION_MANAGER_ADDRESS,
                liquidity_provider,
                nf_token_id,
                row_number() OVER (PARTITION BY pool_address, liquidity_provider, nf_position_manager_address, nf_token_id ORDER BY block_id DESC) AS rn
            FROM {{ src_positions_table }}
            WHERE 
                pool_address IN (SELECT pool_address FROM pool_txs)
                AND nf_position_manager_address IN (SELECT nf_position_manager FROM nf_positions )
                AND nf_token_id IN (SELECT token_id FROM nf_positions ) 
                AND liquidity_provider IS NOT NULL
        ) sq WHERE rn = 1
    ),
    prices AS ({{ safe_ethereum_prices(src_prices_table, '30 days', '9 months') }})
    SELECT 
        'ethereum' as blockchain,
        ee.block_id,
        ee.block_timestamp,
        ee.tx_id,
        ee.event_index,
        contract_address as pool_address,
        p.pool_name,
        coalesce(
            lp_providers.liquidity_provider,
            REGEXP_REPLACE(ee.event_inputs:recipient,'\"','')
        ) as liquidity_provider,
        nf_positions.token_id as nf_token_id,
        nf_positions.nf_position_manager as nf_position_manager_address,
        p.token0_symbol,
        p.token1_symbol,
        CASE WHEN b.amount0_burned IS NOT NULL
            THEN {{ decimal_adjust_with_inputs("ee.event_inputs:amount0", "(ee.event_inputs:amount0 - b.amount0_burned)", "p.token0_decimals") }} 
            ELSE {{ decimal_adjust("ee.event_inputs:amount0", "p.token0_decimals") }} 
        END as amount0_adjusted,
        CASE WHEN b.amount1_burned IS NOT NULL
            THEN {{ decimal_adjust_with_inputs("ee.event_inputs:amount1", "(ee.event_inputs:amount1 - b.amount1_burned)", "p.token1_decimals") }} 
            ELSE {{ decimal_adjust("ee.event_inputs:amount1", "p.token1_decimals") }} 
        END as amount1_adjusted,
        CASE WHEN amount0_adjusted = 0
            THEN 0
            ELSE {{ multiply("amount0_adjusted", "prices_0.price")}}
        END as amount0_usd,
        CASE WHEN amount1_adjusted = 0
            THEN 0
            ELSE {{ multiply("amount1_adjusted", "prices_1.price")}}
        END as amount1_usd,
        event_inputs:tickLower as tick_lower,
        event_inputs:tickUpper as tick_upper,
        CASE WHEN event_inputs:tickLower IS NOT NULL AND p.token1_decimals IS NOT NULL AND p.token0_decimals IS NOT NULL
            THEN pow(1.0001, event_inputs:tickLower) / pow(10, p.token1_decimals - p.token0_decimals)
            ELSE NULL
        END as price_lower,
        CASE WHEN event_inputs:tickUpper IS NOT NULL AND p.token1_decimals IS NOT NULL AND p.token0_decimals IS NOT NULL
            THEN pow(1.0001, event_inputs:tickUpper) / pow(10, p.token1_decimals - p.token0_decimals)
            ELSE NULL
        END as price_upper,
        {{ multiply("price_lower", "prices_1.price")}} as price_lower_usd,
        {{ multiply("price_upper", "prices_1.price")}} as price_upper_usd
    FROM {{ src_events_emitted_table }} ee
    -- Limit collect events to uniswap v3 pools only
    INNER JOIN pools p ON p.pool_address = ee.contract_address
    -- get nf positions in this txs
    LEFT OUTER JOIN nf_positions ON ee.tx_id = nf_positions.tx_id
    -- get lp info in this txs
    LEFT OUTER JOIN 
        lp_providers ON lp_providers.nf_token_id = nf_positions.token_id and 
        lp_providers.nf_position_manager_address = nf_positions.nf_position_manager and 
        lp_providers.pool_address = ee.contract_address
    -- get pool burns in this tx
    LEFT OUTER JOIN burns b ON ee.tx_id = b.tx_id and ee.event_inputs:tickLower = b.tick_lower and ee.event_inputs:tickUpper = b.tick_upper
    -- get USD prices
    LEFT OUTER JOIN prices prices_0
        ON prices_0.hour = date_trunc('day', ee.block_timestamp) AND p.token0 = prices_0.token_address
    LEFT OUTER JOIN prices prices_1
        ON prices_1.hour = date_trunc('day', ee.block_timestamp) AND p.token1 = prices_1.token_address
    -- Filter to Pool Collect events
    WHERE
        event_name = 'Collect'
        -- We don't want collect events where nothing was collected. If there are multiple collect events
        -- in a tx, and there are burn events we want to ensure we're pairing up the burns against collect
        -- events with actual amounts. Otherwise we end up with negative collects.
        and (ee.event_inputs:amount0 > 0 or ee.event_inputs:amount1 > 0)
        AND ee.block_timestamp >= getdate() - interval '9 months'
    HAVING (amount0_adjusted >= 0 or amount1_adjusted >= 0)

{%- endmacro %}