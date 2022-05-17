{{ config(
  materialized = 'view',
  post_hook = "call silver_terra.sp_bulk_get_contract_info()"
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
  UNION ALL
  SELECT
    token_contract AS address
  FROM
    {{ ref("silver_terra__undecoded_token_contracts") }}
)
EXCEPT
SELECT
  address
FROM
  {{ ref("silver_terra__contract_info") }}
