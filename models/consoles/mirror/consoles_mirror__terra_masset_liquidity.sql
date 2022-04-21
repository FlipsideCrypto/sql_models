{{ config(
  materialized = 'table',
  unique_key = 'date',
  incremental_strategy = 'delete+insert',
  cluster_by = ['date'],
  tags = ['snowflake', 'mirror', 'console', 'mirror_terra_masset_liquidity']
) }}

-- original by FlipoCrypto https://app.flipsidecrypto.com/velocity/queries/6fe65fc9-d3dc-4ff4-b40d-42f1034c4192

WITH jdata as (

  SELECT '{
    "vault_data":
    [
     {
        "ADDRESS": "terra1amv303y8kzxuegvurh0gug2xe9wkgj65enq2ux",
        "ADDRESS_NAME": "Terraswap MIR-UST Pair"
      },
      {
        "ADDRESS": "terra1h7t2yq00rxs8a78nyrnhlvp0ewu8vnfnx5efsl",
        "ADDRESS_NAME": "Terraswap mCOIN-UST Pair"
      },
      {
        "ADDRESS": "terra108ukjf6ekezuc52t9keernlqxtmzpj4wf7rx0h",
        "ADDRESS_NAME": "Terraswap mGS-UST Pair"
      },
      {
        "ADDRESS": "terra18cxcwv0theanknfztzww8ft9pzfgkmf2xrqy23",
        "ADDRESS_NAME": "Terraswap mAMD-UST Pair"
      },
      {
        "ADDRESS": "terra1krny2jc0tpkzeqfmswm7ss8smtddxqm3mxxsjm",
        "ADDRESS_NAME": "Terraswap mVIXY-UST Pair"
      },
      {
        "ADDRESS": "terra1a5cc08jt5knh0yx64pg6dtym4c4l8t63rhlag3",
        "ADDRESS_NAME": "Terraswap mARKK-UST Pair"
      },
      {
        "ADDRESS": "terra1prfcyujt9nsn5kfj5n925sfd737r2n8tk5lmpv",
        "ADDRESS_NAME": "Terraswap mBTC-UST Pair"
      },
      {
        "ADDRESS": "terra1vkvmvnmex90wanque26mjvay2mdtf0rz57fm6d",
        "ADDRESS_NAME": "Terraswap mAMZN-UST Pair"
      },
      {
        "ADDRESS": "terra1yl2atgxw422qxahm02p364wtgu7gmeya237pcs",
        "ADDRESS_NAME": "Terraswap mFB-UST Pair"
      },
      {
        "ADDRESS": "terra1yppvuda72pvmxd727knemvzsuergtslj486rdq",
        "ADDRESS_NAME": "Terraswap mNFLX-UST Pair"
      },
      {
        "ADDRESS": "terra1zey9knmvs2frfrjnf4cfv4prc4ts3mrsefstrj",
        "ADDRESS_NAME": "Terraswap mUSO-UST Pair"
      },
      {
        "ADDRESS": "terra17rvtq0mjagh37kcmm4lmpz95ukxwhcrrltgnvc",
        "ADDRESS_NAME": "Terraswap mDOT-UST Pair"
      },
      {
        "ADDRESS": "terra1gq7lq389w4dxqtkxj03wp0fvz0cemj0ek5wwmm",
        "ADDRESS_NAME": "Terraswap mABNB-UST Pair"
      },
      {
        "ADDRESS": "terra1ze5f2lm5clq2cdd9y2ve3lglfrq6ap8cqncld8",
        "ADDRESS_NAME": "Terraswap mGLXY-UST Pair"
      },
      {
        "ADDRESS": "terra15kkctr4eug9txq7v6ks6026yd4zjkrm3mc0nkp",
        "ADDRESS_NAME": "Terraswap mIAU-UST Pair"
      },
      {
        "ADDRESS": "terra17eakdtane6d2y7y6v0s79drq7gnhzqan48kxw7",
        "ADDRESS_NAME": "Terraswap mGME-UST Pair"
      },
      {
        "ADDRESS": "terra1ea9js3y4l7vy0h46k4e5r5ykkk08zc3fx7v4t8",
        "ADDRESS_NAME": "Terraswap mTWTR-UST Pair"
      },
      {
        "ADDRESS": "terra1pdxyk2gkykaraynmrgjfq2uu7r9pf5v8x7k4xk",
        "ADDRESS_NAME": "Terraswap mTSLA-UST Pair"
      },
      {
        "ADDRESS": "terra1u56eamzkwzpm696hae4kl92jm6xxztar9uhkea",
        "ADDRESS_NAME": "Terraswap mGOOGL-UST Pair"
      },
      {
        "ADDRESS": "terra1yngadscckdtd68nzw5r5va36jccjmmasm7klpp",
        "ADDRESS_NAME": "Terraswap mVIXY-UST Pair"
      },
      {
        "ADDRESS": "terra14fyt2g3umeatsr4j4g2rs8ca0jceu3k0mcs7ry",
        "ADDRESS_NAME": "Terraswap mETH-UST Pair"
      },
      {
        "ADDRESS": "terra1afdz4l9vsqddwmjqxmel99atu4rwscpfjm4yfp",
        "ADDRESS_NAME": "Terraswap mBABA-UST Pair"
      },
      {
        "ADDRESS": "terra1774f8rwx76k7ruy0gqnzq25wh7lmd72eg6eqp5",
        "ADDRESS_NAME": "Terraswap mAAPL-UST Pair"
      },
      {
        "ADDRESS": "terra1dkc8075nv34k2fu6xn6wcgrqlewup2qtkr4ymu",
        "ADDRESS_NAME": "Terraswap mQQQ-UST Pair"
      },
      {
        "ADDRESS": "terra1q2cg4sauyedt8syvarc8hcajw6u94ah40yp342",
        "ADDRESS_NAME": "Terraswap mIAU-UST Pair"
      },
      {
        "ADDRESS": "terra14hklnm2ssaexjwkcfhyyyzvpmhpwx6x6lpy39s",
        "ADDRESS_NAME": "Terraswap mSPY-UST Pair"
      },
      {
        "ADDRESS": "terra10ypv4vq67ns54t5ur3krkx37th7j58paev0qhd",
        "ADDRESS_NAME": "Terraswap mMSFT-UST Pair"
      },
      {
        "ADDRESS": "terra1uenpalqlmfaf4efgtqsvzpa3gh898d9h2a232g",
        "ADDRESS_NAME": "Terraswap mAMC-UST Pair"
      },
      {
        "ADDRESS": "terra1f6d9mhrsl5t6yxqnr4rgfusjlt3gfwxdveeyuy",
        "ADDRESS_NAME": "Terraswap mSLV-UST Pair"
      },
      {
        "ADDRESS": "terra1u3pknaazmmudfwxsclcfg3zy74s3zd3anc5m52",
        "ADDRESS_NAME": "Terraswap mSQ-UST Pair"
      },
      {
        "ADDRESS": "terra1lr6rglgd50xxzqe6l5axaqp9d5ae3xf69z3qna",
        "ADDRESS_NAME": "Terraswap mHOOD-UST Pair"
      }
    ]
    }'
 as jdata
)

, labels as (

  SELECT 
    value:ADDRESS::string as ADDRESS,
    value:ADDRESS_NAME::string as ADDRESS_NAME
  FROM jdata, 
  lateral flatten (input => parse_json(jdata):vault_data)

)

, per_lp as (

  SELECT 
    b.date,
    l.ADDRESS_NAME as mAsset,
    CASE 
      WHEN CONTAINS(mAsset, 'LP') THEN REGEXP_SUBSTR(mAsset, 'Terraswap (\\w+)-UST LP', 1, 1, 'e')
      WHEN CONTAINS(mAsset, 'Pair') THEN REGEXP_SUBSTR(mAsset, 'Terraswap (\\w+)-UST Pair', 1, 1, 'e')
      ELSE mAsset
    END AS base_masset,
    balance_usd,
    balance
  FROM {{ ref('terra__daily_balances') }} b 
  
  INNER JOIN labels l 
    ON b.address = l.address
  
  WHERE b.balance_usd > 0 
    AND currency = 'UST' 
    AND b.date >= CURRENT_DATE - 90

  {% if is_incremental() %}
    AND b.date :: DATE >= (SELECT MAX( block_timestamp :: DATE )FROM {{ ref('silver_terra__msgs') }})
  {% endif %}

)

SELECT 
  to_char(date, 'YYYY-MM-DD HH24:MI:SS') AS date, 
  base_masset, 
  SUM(balance) * 2 as TVL
FROM per_lp
GROUP BY 1, 2

UNION

SELECT 
  to_char(date, 'YYYY-MM-DD HH24:MI:SS') AS date, 
  'Total' as base_masset, 
  SUM(balance) * 2 as TVL
FROM per_lp
GROUP BY 1, 2
