{{ config(
  materialized = 'incremental',
  unique_key = "token_contract",
  incremental_strategy = 'merge',
  tags = ['snowflake', 'terra', 'undecoded', 'terra_contracts', 'terra_token_contracts']
) }}

WITH tokens AS (
SELECT
    'UNDECODED CW20' AS description,
    SYSDATE() AS _inserted_timestamp,
    event_attributes :contract_address :: STRING AS token_contract
  FROM
    {{ ref('silver_terra__msg_events') }}
  WHERE
    event_type = 'instantiate_contract'
    AND event_attributes :contract_address IS NOT NULL
  AND tx_status = 'SUCCEEDED'
  AND token_contract NOT IN (
    SELECT
      address
    FROM
      {{ ref('silver_terra__contract_info') }}
  )

  {% if is_incremental() %}
AND _inserted_timestamp >= COALESCE(
  (
    SELECT
      MAX(_inserted_timestamp)
    FROM
      {{ this }}
  ),
  '1900-01-01'
)
{% endif %}


UNION

  SELECT
    'UNDECODED CW20' AS description,
    SYSDATE() AS _inserted_timestamp,
    e.value::STRING AS token_contract
  FROM
    {{ ref('silver_terra__msg_events') }},
  lateral flatten (input => event_attributes) e
  WHERE
    event_type = 'instantiate_contract'
  AND e.key LIKE  '%_contract_address'
    AND tx_status = 'SUCCEEDED'
  AND token_contract NOT IN (
    SELECT
      address
    FROM
      {{ ref('silver_terra__contract_info') }}
  )
  {% if is_incremental() %}
AND _inserted_timestamp >= COALESCE(
  (
    SELECT
      MAX(_inserted_timestamp)
    FROM
      {{ this }}
  ),
  '1900-01-01'
)
{% endif %}
)

SELECT DISTINCT
    description,
    _inserted_timestamp,
    token_contract
FROM tokens
WHERE token_contract IN (
 'terra1hzh9vpxhsk8253se0vv5jj6etdvxu3nv8z07zu',
'terra13zaagrrrxj47qjwczsczujlvnnntde7fdt0mau',
'terra1tlgelulz9pdkhls6uglfn5lmxarx7f2gxtdzh2',
'terra1php5m8a6qd68z02t3zpw4jv2pj4vgw4wz0t8mz',
'terra1042wzrwg2uk6jqxjm34ysqquyr9esdgm5qyswz',
'terra1w0p5zre38ecdy3ez8efd5h9fvgum5s206xknrg',
'terra1zsaswh926ey8qa5x4vj93kzzlfnef0pstuca0y',
'terra1vchw83qt25j89zqwdpmdzj722sqxthnckqzxxp',
'terra10f2mt82kjnkxqj2gepgwl637u2w4ue2z5nhz5j',
'terra178v546c407pdnx5rer3hu8s2c0fc924k74ymnn',
'terra17wkadg0tah554r35x6wvff0y5s7ve8npcjfuhz',
'terra16hhxhyg5eapesc7yw4j5vymr4sqc87fg89a8pq',
'terra1ahygklrmejad3mmeugsd0m5lczv9jl8hjepp6r',
'terra13rprqshf7dxn48v0aaztv5c7ytct6t6u7djtlw',
'terra1sq2ulnxng3ax08prndvwmv5rk9cy5kac2u9n02',
'terra15ytncdwh7yqttvvxjmw7cd2phj9a7cyldv8547',
'terra1z09gnzufuflz6ckd9k0u456l9dnpgsynu0yyhe',
'terra1wrqm3jzyustdme0vz4t2n8jefz9y6wkl3g39gg',
'terra188w26t95tf4dz77raftme8p75rggatxjxfeknw',
'terra1cuku0vggplpgfxegdrenp302km26symjk4xxaf',
'terra12qr3vqxznkjtyrgyrhluh8zlcy82cxv2l92rgh',
'terra1xpueuqns7n6rxfuagcpphm77dfyn05fe0cjfm0',
'terra1pg606jw68d9mnh9czrgm7celc3rq9x5wrvj7gl',
'terra1ufhsnrefjazjtyjh2mwxcnsrejmvqvdrhevph2',
'terra1tndcaqxkpc5ce9qee5ggqf430mr2z3pefe5wj6',
'terra1y35nf8sk82pc2h8fh2cmud2ku6mwjc3r36p63q',
'terra1rrek47kf0t968g2jcg5ww8fw58n93jkclkk4v5',
'terra1cqldrws5n6l8te9sh7uv86v64dffjtg3ldqksq',
'terra1gecs98vcuktyfkrve9czrpgtg0m3aq586x6gzm',
'terra1au93h9ek7flj6f6gar0m8g6ee0nyahd5a6j9uf')