{{ config(
    materialized = 'view', 
    tags = ['snowflake', 'ethereum', 'gold_ethereum', 'ethereum_contracts']
) }}

SELECT
    address,
    meta,
    NAME
FROM 
    {{ ref('silver_ethereum__contracts') }}