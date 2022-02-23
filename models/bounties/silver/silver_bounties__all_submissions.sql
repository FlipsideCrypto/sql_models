{{ config(
  materialized = 'incremental',
  unique_key = 'id',
  incremental_strategy = 'merge',
  tags = ['snowflake', 'bi_analytics', 'airtable', 'bounties']
) }}

SELECT
  id,
  TIMESTAMP AS submitted_timestamp,
  program_name,
  program_type,
  question_difficulty,
  email_address,
  date_of_users_first_appearance,
  date_of_users_last_appearance,
  discord_handle,
  twitter_handles,
  links_to_public_results,
  total_score,
  grand_prize_winner,
  wallet_address,
  amount_to_pay_automated,
  token_to_pay,
  finance_has_paid,
  bounty_question_title
FROM
  {{ source(
    'bi_analytics',
    'all_submissions'
  ) }}
