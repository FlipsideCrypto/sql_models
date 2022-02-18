{{ config(
  materialized = 'incremental',
  unique_key = 'id',
  incremental_strategy = 'merge',
  tags = ['snowflake', 'bi_analytics', 'airtable']
) }}

SELECT
  id,
  timestamp_submission_sheet,
  program_name,
  program_type,
  -- question level?
  question_difficulty,
  email_address,
  date_of_users_first_appearance,
  -- date of last submission?
  date_of_users_last_appearance,
  discord_handle,
  twitter_handles,
  -- submission link?
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
