{{ config(
  materialized = 'view',
  tags = ['snowflake', 'algorand_views', 'labels', 'algorand_labels', 'address_labels']
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
  blockchain = 'algorand'
UNION
SELECT
  blockchain,
  creator,
  address,
  label_type,
  label_subtype,
  label,
  address_name
FROM
  {{ ref('silver_algorand__pool_addresses') }}
