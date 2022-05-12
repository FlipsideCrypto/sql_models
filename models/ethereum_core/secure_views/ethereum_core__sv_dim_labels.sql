{{ config(
    materialized = 'view',
    secure = true,
    tags = ['snowflake', 'ethereum', 'gold_ethereum', 'new_eth']
) }}

SELECT
    blockchain,
    creator,
    address,
    address_name,
    label_type,
    label_subtype,
    label
FROM
    {{ ref('ethereum__labels') }}
WHERE
    blockchain = 'ethereum'
    AND address LIKE '0x%'
