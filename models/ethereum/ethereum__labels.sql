{{ config(
  materialized = 'view',
  tags = ['snowflake', 'ethereum_views', 'labels', 'ethereum_labels', 'address_labels']
) }}

SELECT
  blockchain,
  creator,
  address,
  l1_label AS label_type,
  l2_label AS label_subtype,
  project_name AS label,
  address_name AS address_name
FROM
  {{ ref('silver_crosschain__address_labels') }}
WHERE
  blockchain = 'ethereum'
  AND LOWER(address) LIKE '0x%'
