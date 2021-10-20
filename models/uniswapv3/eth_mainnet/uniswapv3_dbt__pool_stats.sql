{{ config(
    materialized = 'incremental',
    unique_key = 'tx_id || nf_token_id',
    incremental_strategy = 'delete+insert',
    tags = ['snowflake', 'uniswapv3_silver', 'uniswapv3_dbt__liquidity_actions']
) }}

WITH base_tables AS (

    SELECT
        *
    FROM
        {{ source(
            'bronze',
            'prod_ethereum_sink_407559501'
        ) }}
    WHERE
        record_content :model :name :: STRING IN (
            'uniswap_v3_mainnet_liquidity_action_model'
        )

{% if is_incremental() %}
AND (
    record_metadata :CreateTime :: INT / 1000
) :: TIMESTAMP :: DATE >= (
    SELECT
        DATEADD('day', -1, MAX(system_created_at :: DATE))
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    (
        record_metadata :CreateTime :: INT / 1000
    ) :: TIMESTAMP AS system_created_at,
    t.value :block_id :: INTEGER AS block_id,
    t.value :block_timestamp :: TIMESTAMP AS block_timestamp,
    t.value :blockchain :: STRING AS blockchain,
    t.value :fee_growth_global_0_x128 :: FLOAT AS fee_growth_global_0_x128,
    t.value :fee_growth_global_1_x128 :: FLOAT AS fee_growth_global_1_x128,
    t.value :observation_cardinality :: INTEGER AS observation_cardinality,
    t.value :observation_cardinality_next :: INTEGER AS observation_cardinality_next,
    t.value :observation_index :: INTEGER AS observation_index,
    t.value :pool_address :: STRING AS pool_address,
    t.value :price :: FLOAT AS price,
    t.value :price_0_1 :: FLOAT AS price_0_1,
    t.value :price_1_0 :: FLOAT AS price_1_0,
    t.value :protocol_fees_token0 :: FLOAT AS protocol_fees_token0,
    t.value :protocol_fees_token1 :: FLOAT AS protocol_fees_token1,
    t.value :sqrt_price_x96 :: FLOAT AS sqrt_price_x96,
    t.value :tick :: INTEGER AS tick,
    t.value :token0_balance :: FLOAT AS token0_balance,
    t.value :token1_balance :: FLOAT AS token1_balance,
    t.value :unlocked :: BOOLEAN AS unlocked,
    t.value :virtual_liquidity :: FLOAT AS virtual_liquidity,
    t.value :virtual_liquidity_adjusted :: FLOAT AS virtual_liquidity_adjusted,
    t.value :virtual_reserves_token0 :: FLOAT AS virtual_reserves_token0,
    t.value :virtual_reserves_token1 :: FLOAT AS virtual_reserves_token1
FROM
    base_tables,
    LATERAL FLATTEN(
        input => record_content :results
    ) t
