{{ config(
    materialized = 'view', 
    tags = ['snowflake', 'crosschain', 'labels', 'gold_address_labels'], 
) }}

SELECT 
    system_created_at, 
    insert_date, 
    blockchain, 
    address, 
    creator, 
    l1_label AS label_type, 
    l2_label AS label_subtype, 
    address_name, 
    project_name
FROM 
    {{ ref('silver_crosschain__address_labels') }}