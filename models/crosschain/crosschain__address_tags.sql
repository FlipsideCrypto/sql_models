{{ config(
    materialized = 'view', 
    tags = ['snowflake', 'crosschain', 'address_tags', 'tags'], 
) }}

SELECT 
    blockchain, 
    creator, 
    address, 
    tag_name, 
    tag_type, 
    start_date, 
    end_date
FROM {{ ref('silver_crosschain__address_tags') }}