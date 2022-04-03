{{ config(
  materialized = 'table',
  unique_key = 'date',
  incremental_strategy = 'delete+insert',
  cluster_by = ['date'],
  tags = ['snowflake', 'mirror', 'console', 'mirror_terra_total_gas_fee']
) }}

WITH jdata as (

  SELECT '{
    "vault_data":
    [
     {
        "ADDRESS": "terra1227ppwxxj3jxz8cfgq00jgnxqcny7ryenvkwj6",
        "ADDRESS_NAME": "mMSFT"
      },
      {
        "ADDRESS": "terra14y5affaarufk3uscy2vr6pe6w6zqf2wpjzn5sh",
        "ADDRESS_NAME": "mTSLA"
      },
      {
        "ADDRESS": "terra15gwkyepfc6xgca5t5zefzwy42uts8l2m4g40k6",
        "ADDRESS_NAME": "MIR"
      },
      {
        "ADDRESS": "terra1amv303y8kzxuegvurh0gug2xe9wkgj65enq2ux",
        "ADDRESS_NAME": "Terraswap MIR-UST Pair"
      },
      {
        "ADDRESS": "terra17gjf2zehfvnyjtdgua9p9ygquk6gukxe7ucgwh",
        "ADDRESS_NAME": "Terraswap MIR-UST LP"
      },
      {
        "ADDRESS": "terra15hp9pr8y4qsvqvxf3m4xeptlk7l8h60634gqec",
        "ADDRESS_NAME": "mIAU"
      },
      {
        "ADDRESS": "terra165nd2qmrtszehcfrntlplzern7zl4ahtlhd5t2",
        "ADDRESS_NAME": "mAMZN"
      },
      {
        "ADDRESS": "terra18wayjpyq28gd970qzgjfmsjj7dmgdk039duhph",
        "ADDRESS_NAME": "mCOIN"
      },
      {
        "ADDRESS": "terra1cc3enj9qgchlrj34cnzhwuclc4vl2z3jl7tkqg",
        "ADDRESS_NAME": "mTWTR"
      },
      {
        "ADDRESS": "terra1csk6tc7pdmpr782w527hwhez6gfv632tyf72cp",
        "ADDRESS_NAME": "mQQQ"
      },
      {
        "ADDRESS": "terra1h8arz2k547uvmpxctuwush3jzc8fun4s96qgwt",
        "ADDRESS_NAME": "mGOOGL"
      },
      {
        "ADDRESS": "terra1jsxngqasf2zynj5kyh0tgq9mj3zksa5gk35j4k",
        "ADDRESS_NAME": "mNFLX"
      },
      {
        "ADDRESS": "terra1kscs6uhrqwy6rx5kuw5lwpuqvm3t6j2d6uf2lp",
        "ADDRESS_NAME": "mSLV"
      },
      {
        "ADDRESS": "terra1lvmx8fsagy70tv0fhmfzdw9h6s3sy4prz38ugf",
        "ADDRESS_NAME": "mUSO"
      },
      {
        "ADDRESS": "terra1mqsjugsugfprn3cvgxsrr8akkvdxv2pzc74us7",
        "ADDRESS_NAME": "mFB"
      },
      {
        "ADDRESS": "terra1vxtwu4ehgzz77mnfwrntyrmgl64qjs75mpwqaz",
        "ADDRESS_NAME": "mAAPL"
      },
      {
        "ADDRESS": "terra1w7zgkcyt7y4zpct9dw8mw362ywvdlydnum2awa",
        "ADDRESS_NAME": "mBABA"
      },
      {
        "ADDRESS": "terra1zp3a6q6q4953cz376906g5qfmxnlg77hx3te45",
        "ADDRESS_NAME": "mVIXY"
      },
      {
        "ADDRESS": "terra1g4x2pzmkc9z3mseewxf758rllg08z3797xly0n",
        "ADDRESS_NAME": "mABNB"
      },
      {
        "ADDRESS": "terra1qelfthdanju7wavc5tq0k5r0rhsyzyyrsn09qy",
        "ADDRESS_NAME": "mAMC"
      },
      {
        "ADDRESS": "terra18ej5nsuu867fkx4tuy2aglpvqjrkcrjjslap3z",
        "ADDRESS_NAME": "mAMD"
      },
      {
        "ADDRESS": "terra1qqfx5jph0rsmkur2zgzyqnfucra45rtjae5vh6",
        "ADDRESS_NAME": "mARKK"
      },
      {
        "ADDRESS": "terra1rhhvx8nzfrx5fufkuft06q5marfkucdqwq5sjw",
        "ADDRESS_NAME": "mBTC"
      },
      {
        "ADDRESS": "terra19ya4jpvjvvtggepvmmj6ftmwly3p7way0tt08r",
        "ADDRESS_NAME": "mDOT"
      },
      {
        "ADDRESS": "terra1dk3g53js3034x4v5c3vavhj2738une880yu6kx",
        "ADDRESS_NAME": "mETH"
      },
      {
        "ADDRESS": "terra1l5lrxtwd98ylfy09fn866au6dp76gu8ywnudls",
        "ADDRESS_NAME": "mGLXY"
      },
      {
        "ADDRESS": "terra1m6j6j9gw728n82k78s0j9kq8l5p6ne0xcc820p",
        "ADDRESS_NAME": "mGME"
      },
      {
        "ADDRESS": "terra137drsu8gce5thf6jr5mxlfghw36rpljt3zj73v",
        "ADDRESS_NAME": "mGS"
      },
      {
        "ADDRESS": "terra1aa00lpfexyycedfg5k2p60l9djcmw0ue5l8fhc",
        "ADDRESS_NAME": "mSPY"
      },
      {
        "ADDRESS": "terra1u43zu5amjlsgty5j64445fr9yglhm53m576ugh",
        "ADDRESS_NAME": "mSQ"
      },
      {
        "ADDRESS": "terra18yqdfzfhnguerz9du5mnvxsh5kxlknqhcxzjfr",
        "ADDRESS_NAME": "mHOOD"
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
        "ADDRESS": "terra1jl4vkz3fllvj6fchnj2trrm9argtqxq6335ews",
        "ADDRESS_NAME": "Terraswap mIAU-UST LP"
      },
      {
        "ADDRESS": "terra18cxcwv0theanknfztzww8ft9pzfgkmf2xrqy23",
        "ADDRESS_NAME": "Terraswap mAMD-UST Pair"
      },
      {
        "ADDRESS": "terra1m8mr9u3su46ezxwf7z7xnvm0jsapl2jd8vgefh",
        "ADDRESS_NAME": "Terraswap mAMD-UST LP"
      },
      {
        "ADDRESS": "terra1jmauv302lfvpdfau5nhzy06q0j2f9te4hy2d07",
        "ADDRESS_NAME": "Terraswap mABNB-UST LP"
      },
      {
        "ADDRESS": "terra1jqqegd35rg2gjde54adpj3t6ecu0khfeaarzy9",
        "ADDRESS_NAME": "Terraswap mSPY-UST LP"
      },
      {
        "ADDRESS": "terra1krny2jc0tpkzeqfmswm7ss8smtddxqm3mxxsjm",
        "ADDRESS_NAME": "Terraswap mVIXY-UST Pair"
      },
      {
        "ADDRESS": "terra1veqh8yc55mhw0ttjr5h6g9a6r9nylmrc0nzhr7",
        "ADDRESS_NAME": "Terraswap mARKK-UST LP"
      },
      {
        "ADDRESS": "terra1a5cc08jt5knh0yx64pg6dtym4c4l8t63rhlag3",
        "ADDRESS_NAME": "Terraswap mARKK-UST Pair"
      },
      {
        "ADDRESS": "terra1ktckr8v7judrr6wkwv476pwsv8mht0zqzw2t0h",
        "ADDRESS_NAME": "Terraswap mCOIN-UST LP"
      },
      {
        "ADDRESS": "terra1mtvslkm2tgsmh908dsfksnqu7r7lulh24a6knv",
        "ADDRESS_NAME": "Terraswap mAMC-UST LP"
      },
      {
        "ADDRESS": "terra1pjgzke6h5v4nz978z3a92gqajwhn8yyh5kv4zv",
        "ADDRESS_NAME": "Terraswap mGLXY-UST LP"
      },
      {
        "ADDRESS": "terra1prfcyujt9nsn5kfj5n925sfd737r2n8tk5lmpv",
        "ADDRESS_NAME": "Terraswap mBTC-UST Pair"
      },
      {
        "ADDRESS": "terra1q7m2qsj3nzlz5ng25z5q5w5qcqldclfe3ljup9",
        "ADDRESS_NAME": "Terraswap mAMZN-UST LP"
      },
      {
        "ADDRESS": "terra1stfeev27wdf7er2uja34gsmrv58yv397dlxmyn",
        "ADDRESS_NAME": "Terraswap mBABA-UST LP"
      },
      {
        "ADDRESS": "terra1utf3tm35qk6fkft7ltcnscwml737vfz7xghwn5",
        "ADDRESS_NAME": "Terraswap mUSO-UST LP"
      },
      {
        "ADDRESS": "terra1vkvmvnmex90wanque26mjvay2mdtf0rz57fm6d",
        "ADDRESS_NAME": "Terraswap mAMZN-UST Pair"
      },
      {
        "ADDRESS": "terra1ygazp9w7tx64rkx5wmevszu38y5cpg6h3fk86e",
        "ADDRESS_NAME": "Terraswap mTSLA-UST LP"
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
        "ADDRESS": "terra14uaqudeylx6tegamqmygh85lfq8qg2jmg7uucc",
        "ADDRESS_NAME": "Terraswap mMSFT-UST LP"
      },
      {
        "ADDRESS": "terra17rvtq0mjagh37kcmm4lmpz95ukxwhcrrltgnvc",
        "ADDRESS_NAME": "Terraswap mDOT-UST Pair"
      },
      {
        "ADDRESS": "terra1p60datmmf25wgssguv65ltds3z6ea3me74nm2e",
        "ADDRESS_NAME": "Terraswap mDOT-UST LP"
      },
      {
        "ADDRESS": "terra16auz7uhnuxrj2dzrynz2elthx5zpps5gs6tyln",
        "ADDRESS_NAME": "Terraswap mETH-UST LP"
      },
      {
        "ADDRESS": "terra17smg3rl9vdpawwpe7ex4ea4xm6q038gp2chge5",
        "ADDRESS_NAME": "Terraswap mGS-UST LP"
      },
      {
        "ADDRESS": "terra1cmrl4txa7cwd7cygpp4yzu7xu8g7c772els2y8",
        "ADDRESS_NAME": "Terraswap mVIXY-UST LP"
      },
      {
        "ADDRESS": "terra1falkl6jy4087h4z567y2l59defm9acmwcs70ts",
        "ADDRESS_NAME": "Terraswap mGOOGL-UST LP"
      },
      {
        "ADDRESS": "terra1fc5a5gsxatjey9syq93c2n3xq90n06t60nkj6l",
        "ADDRESS_NAME": "Terraswap mTWTR-UST LP"
      },
      {
        "ADDRESS": "terra1gq7lq389w4dxqtkxj03wp0fvz0cemj0ek5wwmm",
        "ADDRESS_NAME": "Terraswap mABNB-UST Pair"
      },
      {
        "ADDRESS": "terra1jh2dh4g65hptsrwjv53nhsnkwlw8jdrxaxrca0",
        "ADDRESS_NAME": "Terraswap mFB-UST LP"
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
        "ADDRESS": "terra16j09nh806vaql0wujw8ktmvdj7ph8h09ltjs2r",
        "ADDRESS_NAME": "Terraswap mQQQ-UST LP"
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
        "ADDRESS": "terra178cf7xf4r9d3z03tj3pftewmhx0x2p77s0k6yh",
        "ADDRESS_NAME": "Terraswap mSLV-UST LP"
      },
      {
        "ADDRESS": "terra1afdz4l9vsqddwmjqxmel99atu4rwscpfjm4yfp",
        "ADDRESS_NAME": "Terraswap mBABA-UST Pair"
      },
      {
        "ADDRESS": "terra122asauhmv083p02rhgyp7jn7kmjjm4ksexjnks",
        "ADDRESS_NAME": "Terraswap mAAPL-UST LP"
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
        "ADDRESS": "terra1mwu3cqzvhygqg7vrsa6kfstgg9d6yzkgs6yy3t",
        "ADDRESS_NAME": "Terraswap mNFLX-UST LP"
      },
      {
        "ADDRESS": "terra1d34edutzwcz6jgecgk26mpyynqh74j3emdsnq5",
        "ADDRESS_NAME": "Terraswap mBTC-UST LP"
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
        "ADDRESS": "terra1azk43zydh3sdxelg3h4csv4a4uef7fmjy0hu20",
        "ADDRESS_NAME": "Terraswap mGME-UST LP"
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
        "ADDRESS": "terra1mv3pgkzs4krcennqj442jscg6jv84cejrs50n2",
        "ADDRESS_NAME": "Terraswap mSQ-UST LP"
      },
      {
        "ADDRESS": "terra1lr6rglgd50xxzqe6l5axaqp9d5ae3xf69z3qna",
        "ADDRESS_NAME": "Terraswap mHOOD-UST Pair"
      },
      {
        "ADDRESS": "terra1s0dgcsdy9kgunnf3gnwl40uwy9rxtmc39mhy2m",
        "ADDRESS_NAME": "Terraswap mHOOD-UST LP"
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
  
, gas_tx as (
  SELECT 
    date_trunc('day', block_timestamp) as date, 
    TX_ID, 
    IFF(FEE[0]:amount[0] IS NOT NULL, FEE[0]:amount[0]:amount::decimal, FEE[0]:amount) as raw_amount, --Consider changes post Col-5 in transactions
  	 raw_amount / POW (10, 6) as amount, IFF(FEE[0]:amount[0] IS NOT NULL, FEE[0]:amount[0]:denom::string, FEE[0]:denom::string) as denom
  FROM {{ ref('terra__transactions') }}
  WHERE TX_STATUS = 'SUCCEEDED' 
    AND (FEE[0]:amount[0] IS NOT NULL 
        OR FEE[0]:amount IS NOT NULL) 
    AND FEE[0]:amount <> '[]' --Check for post-Col 5 changes in transactions contract

  {% if is_incremental() %}
    AND block_timestamp :: DATE >= (SELECT MAX( block_timestamp :: DATE )FROM {{ ref('silver_terra__msgs') }})
  {% endif %}


)

, prices as (
  SELECT 
    date_trunc('day', block_timestamp) as date, 
    currency, 
    symbol, 
    avg(price_usd) as conversion_price_usd
  FROM {{ ref('terra__oracle_prices') }}
  WHERE currency IS NOT NULL

  {% if is_incremental() %}
    AND block_timestamp :: DATE >= (SELECT MAX( block_timestamp :: DATE )FROM {{ ref('silver_terra__msgs') }})
  {% endif %}

  GROUP BY 1, 2, 3
)

, gas_conversion as (
  SELECT 
    g.date, 
    TX_ID, 
    amount * conversion_price_usd as gas_usd, 
    symbol
  FROM gas_tx g 
  
  INNER JOIN prices p 
    ON g.date = p.date 
    AND g.denom = p.currency
  
)

, mirror_tx as (
  SELECT 
    date_trunc('day', block_timestamp) as date,
    TX_ID,
    MSG_VALUE,
    MSG_VALUE:contract::string as contract,
    l.ADDRESS_NAME as mAsset,
  	CASE 
	  WHEN CONTAINS(mAsset, 'LP') THEN REGEXP_SUBSTR(mAsset, 'Terraswap (\\w+)-UST LP', 1, 1, 'e')
	  WHEN CONTAINS(mAsset, 'Pair') THEN REGEXP_SUBSTR(mAsset, 'Terraswap (\\w+)-UST Pair', 1, 1, 'e')
	  ELSE mAsset
    END as base_masset
  FROM {{ ref('terra__msgs') }} m
  
  INNER JOIN labels l 
    ON MSG_VALUE:contract::string = l.ADDRESS 
  WHERE tx_status = 'SUCCEEDED' 
    AND MSG_TYPE = 'wasm/MsgExecuteContract' 

  {% if is_incremental() %}
    AND m.block_timestamp :: DATE >= (SELECT MAX( block_timestamp :: DATE )FROM {{ ref('silver_terra__msgs') }})
  {% endif %}

)

, daily_sum as (
  SELECT 
    to_char(m.date, 'YYYY-MM-DD') as date, 
    base_masset, 
    sum(gas_usd) as tot_gas_usd
  FROM mirror_tx m 
  
  INNER JOIN gas_conversion g 
    ON m.tx_id = g.tx_id
  GROUP BY 1, 2

)

SELECT 
  date, 
  base_masset, 
  tot_gas_usd
FROM daily_sum

