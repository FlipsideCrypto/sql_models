{{ config(
    materialized = 'incremental',
    unique_key = 'blockhour || token_address',
    incremental_strategy = 'delete+insert',
    tags = ['snowflake', 'ethereum', 'aave', 'aave_liquidity_mining', 'atb_test']
) }}

WITH aave_base AS (

    SELECT
        *
    FROM
        {{ source(
            'bronze',
            'prod_ethereum_sink_407559501'
        ) }}
    WHERE
        record_content :model :class = 'aave.emission_speed_reward_rate.aave_emission_rate.AaveEmissionModel'

{% if is_incremental() %}
AND (
    record_metadata :CreateTime :: INT / 1000
) :: TIMESTAMP :: DATE >= (
    SELECT
        DATEADD('day', -1, MAX(blockhour :: DATE))
    FROM
        {{ this }}
)
{% endif %}
),
aave_reads AS (
    SELECT
        (
            record_metadata :CreateTime :: INT / 1000
        ) :: TIMESTAMP AS system_created_at,
        t.value :block_id :: bigint AS block_id,
        t.value :block_timestamp :: TIMESTAMP AS block_timestamp,
        t.value :contract_address :: STRING AS contract_address,
        LOWER(
            SUBSTRING(
                t.value :inputs :: STRING,
                13,
                42
            )
        ) AS token_address,
        t.value :value_string :: STRING AS value_string,
        (SPLIT(LOWER(t.value :value_string :: STRING), '^')) AS aave_read
    FROM
        aave_base,
        LATERAL FLATTEN(
            input => record_content :results
        ) t
),
long_format AS (
    SELECT
        system_created_at,
        block_id,
        block_timestamp,
        contract_address,
        token_address,
        value_string,
        (SPLIT(LOWER(VALUE :: STRING), ':')) [0] AS field_name,
        (SPLIT(LOWER(VALUE :: STRING), ':')) [1] AS VALUE
    FROM
        aave_reads,
        TABLE(FLATTEN(aave_reads.aave_read))
),
wide_format AS (
    SELECT
        *
    FROM
        long_format pivot(MAX(VALUE) for field_name IN ('emissionpersecond', 'index', 'lastupdatetimestamp'))
)
SELECT
    DISTINCT DATE_TRUNC(
        'hour',
        block_timestamp
    ) AS blockhour,
    token_address,
    (
        "'emissionpersecond'" :: numeric
    ) / power(
        10,
        18
    ) AS emissionpersecond
FROM
    wide_format w
WHERE
    "'emissionpersecond'" :: numeric > 0
