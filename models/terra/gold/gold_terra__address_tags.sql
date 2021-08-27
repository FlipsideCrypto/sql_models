{{ config(
    materialized = 'table',
    sort = 'address'
) }}

SELECT
    blockchain,
    address,
    tag_name,
    tag_type,
    tag_slug,
    source,
    start_date,
    end_date
FROM
    {{ source(
        'shared',
        'udm_address_tags'
    ) }}
WHERE
    blockchain = 'terra'
