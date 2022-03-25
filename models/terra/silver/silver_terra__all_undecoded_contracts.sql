{{ config(
  materialized = 'view',
) }}
(

  SELECT
    token_contract AS address
  FROM
    {{ ref("silver_terra__undecoded_oracle_contracts") }}
  UNION ALL
  SELECT
    token_contract AS address
  FROM
    {{ ref("silver_terra__undecoded_wormhole_contracts") }}
)
EXCEPT
SELECT
  address
FROM
  {{ ref("silver_terra__contract_info") }}
