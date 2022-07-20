{{ config(
    materialized = 'incremental',
    unique_key = 'dim_swap_program_id',
    incremental_strategy = 'merge'
) }}

WITH base AS (

    SELECT
        'algofi' swap_program,
        'https://app.algofi.org/' AS url
    UNION ALL
    SELECT
        'pactfi',
        'https://app.pact.fi/'
    UNION ALL
    SELECT
        'wagmiswap',
        'https://app.wagmiswap.io/'
    UNION ALL
    SELECT
        'tinyman',
        'https://app.tinyman.org/'
    UNION ALL
    SELECT
        NULL,
        NULL
)
SELECT
    {{ dbt_utils.surrogate_key(
        ['swap_program']
    ) }} AS dim_swap_program_id,
    swap_program,
    url
FROM
    base
