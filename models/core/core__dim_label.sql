{{ config(
  materialized = 'view'
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
  {{ source(
    'crosschain',
    'address_labels'
  ) }}
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
  {{ ref('silver__pool_addresses') }}
