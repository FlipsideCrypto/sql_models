{{ config(
  materialized = 'view',
  unique_key = 'MONTH',
  tags = ['snowflake', 'terra', 'console', 'terra_monthly_active_users']
) }}


WITH active_wallets AS (

    SELECT
        date_trunc('month', block_timestamp) as date,
        coalesce(
            msg_value:sender,
            msg_value: msgs[0]:"from_address", -- Used by authz/MsgExec
            msg_value: granter, -- Used by 'authz/MsgGrant', 'feegrant/MsgGrantAllowance'
            msg_value: depositor, -- Used by gov/MsgDeposit
  			msg_value: proposer, -- gov/MsgSubmitProposal
  			msg_value: voter, -- 'gov/MsgVote'
  			msg_value: trader, -- market/MsgSwap
            msg_value: from_address,
            msg_value: to_address,
            msg_value: delegator_address, -- 'staking/MsgBeginRedelegate','staking/MsgCreateValidator','staking/MsgDelegate','staking/MsgUndelegate'
            msg_value: owner,
            msg_value: signer
        ) as address
    FROM {{ ref('terra__msgs') }}
    WHERE tx_status = 'SUCCEEDED'
      AND address IS NOT NULL
      AND date >= CURRENT_DATE - interval '7 month'
  	  AND date < date_trunc('month',CURRENT_DATE)
      AND msg_type IN (
          'applications/transfer.v1.MsgTransfer' -- IBC Application Transfer for user, MsgTransfer defines a msg to transfer fungible tokens (i.e Coins) between ICS20 enabled chains.,
          ,'authz/MsgExec' -- When a grantee wants to execute a transaction on behalf of a granter, they must send MsgExec.,
          ,'authz/MsgGrant' -- MsgGrantAllowance adds permission for Grantee to spend up to Allowance,
          ,'bank/MsgSend' -- Send coins from one address to another.,
          ,'distribution/MsgWithdrawDelegatorReward' -- Under special circumstances a delegator may wish to withdraw rewards from only a single validator.,
          ,'feegrant/MsgGrantAllowance' -- MsgGrantAllowance adds permission for Grantee to spend up to Allowance of fees from the account of Granter.,
          ,'gov/MsgDeposit' -- MsgDeposit defines a message to submit a deposit to an existing proposal.,
          ,'gov/MsgSubmitProposal' -- MsgSubmitProposal defines an sdk.Msg type that supports submitting arbitrary proposal Content.,
          ,'gov/MsgVote' -- MsgVote defines a message to cast a vote.,
          ,'market/MsgSwap' -- A MsgSwap transaction denotes the Trader’s intent to swap their balance of OfferCoin for a new denomination AskDenom.,
          ,'market/MsgSwapSend' -- A MsgSwapSend first swaps OfferCoin for AskDenom and then sends the acquired coins to ToAddress,
          ,'staking/MsgBeginRedelegate' -- MsgBeginRedelegate defines a SDK message for performing a redelegation of coins from a delegator and source validator to a destination validator.,
          ,'staking/MsgCreateValidator' -- MsgCreateValidator defines a SDK message for creating a new validator.,
          ,'staking/MsgDelegate' -- MsgDelegate defines a SDK message for performing a delegation of coins from a delegator to a validator.,
          ,'staking/MsgUndelegate' -- MsgUndelegate defines a SDK message for performing an undelegation from a delegate and a validator.
        )
      AND msg_type NOT IN (
          'core/channel.v1.MsgAcknowledgement' -- MsgAcknowledgement receives incoming IBC acknowledgement,
          ,'core/channel.v1.MsgChannelOpenConfirm' -- MsgChannelOpenConfirm defines a msg sent by a Relayer to Chain B to acknowledge the change of channel state to OPEN on Chain A.,
          ,'core/channel.v1.MsgChannelOpenTry' -- MsgChannelOpenInit defines a msg sent by a Relayer to try to open a channel on Chain B.,
          ,'core/channel.v1.MsgRecvPacket' -- MsgRecvPacket receives incoming IBC packet,
          ,'core/channel.v1.MsgTimeout' -- MsgTimeout receives timed-out packet,
          ,'core/client.v1.MsgCreateClient' -- MsgCreateClient defines a message to create an IBC client,
          ,'core/client.v1.MsgUpdateClient' -- MsgUpdateClient defines an sdk.Msg to update a IBC client state using the given header.,
          ,'core/connection.v1.MsgConnectionOpenConfirm' -- MsgConnectionOpenConfirm defines a msg sent by a Relayer to Chain B to acknowledge the change of connection state to OPEN on Chain A.,
          ,'core/connection.v1.MsgConnectionOpenTry' -- MsgConnectionOpenTry defines a msg sent by a Relayer to try to open a connection on Chain B.,
          ,'distribution/MsgWithdrawValidatorCommission' -- MsgWithdrawValidatorCommission withdraws the full commission to the validator address.,
          ,'oracle/MsgAggregateExchangeRatePrevote' -- MsgExchangeRatePrevote - struct for prevoting on the ExchangeRateVote.,
          ,'oracle/MsgAggregateExchangeRateVote' -- Used by Validators,
          ,'oracle/MsgDelegateFeedConsent' -- Validators may also elect to delegate voting rights to another key to prevent the block signing key from being kept online. To do so, they must submit a MsgDelegateFeedConsent,,
          ,'slashing/MsgUnjail' -- MsgUnjail defines the Msg/Unjail request type for Validator,
          ,'staking/MsgEditValidator' -- MsgEditValidator defines a SDK message for editing an existing validator, no wallet address.,
          ,'wasm/MsgExecuteContract' -- Invokes a function defined within the smart contract. Function and parameters are encoded in ExecuteMsg, which is a JSON message encoded in Base64.,
          ,'wasm/MsgInstantiateContract' -- Creates a new instance of a smart contract. Initial configuration is provided in the InitMsg, which is a JSON message encoded in Base64.,
          ,'wasm/MsgMigrateContract' -- Can be issued by the owner of a migratable smart contract to reset its code ID to another one. MigrateMsg is a JSON message encoded in Base64.,
          ,'wasm/MsgStoreCode' -- Uploads new code to the blockchain and results in a new code ID, if successful. WASMByteCode is accepted as either uncompressed or gzipped binary data encoded as Base64.,
          ,'wasm/MsgUpdateContractAdmin' -- Couldn't find information but looks like something to do with contract update.
        )
      AND regexp_like(address, 'terra1[a-z0-9]{38}') -- https://docs.terra.money/docs/develop/sdks/terra-js/common-examples.html#validate-a-terra-address

),

active_wallets_multisend AS (

  SELECT
    	 date_trunc('month', block_timestamp) as date,
    	 value:address as address
  FROM {{ ref('terra__msgs') }}
  , lateral flatten(input => msg_value:inputs)
  WHERE tx_status = 'SUCCEEDED'
    AND address IS NOT NULL
    AND date >= CURRENT_DATE - interval '7 month'
    AND date < date_trunc('month',CURRENT_DATE)
    AND msg_type = 'bank/MsgMultiSend'
    AND regexp_like(address,'terra1[a-z0-9]{38}')

),

monthly_unique as (

    SELECT
        date as "MONTH",
        count(distinct(address)) as "ACTIVE_USERS"
    FROM (
  		SELECT * FROM active_wallets
  		UNION 
        SELECT * FROM active_wallets_multisend
    )
    GROUP BY
        "MONTH"

)

SELECT
    TO_CHAR("MONTH",'mon yyyy') as "MONTH", 
    "ACTIVE_USERS"
FROM
    monthly_unique