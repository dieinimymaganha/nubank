WITH transfer_balance AS (
  SELECT
    account_id,
    month_id,
    year_id,
    SUM(CASE WHEN transfer_type = 'in' THEN amount ELSE -amount END) AS balance
  FROM (
    SELECT
      account_id,
      extract(month from action_timestamp) AS month_id,
      extract(year from action_timestamp) AS year_id,
      transaction_requested_at,
      transaction_completed_at,
      amount,
      'in' as transfer_type
    FROM transfer_ins
    JOIN d_time ON transaction_requested_at = time_id
    WHERE status = 'completed'
    UNION
    SELECT
      account_id,
      extract(month from action_timestamp) AS month_id,
      extract(year from action_timestamp) AS year_id,
      transaction_requested_at,
      transaction_completed_at,
      amount,
      'out' as transfer_type
    FROM transfer_outs
    JOIN d_time ON transaction_requested_at = time_id
    WHERE status = 'completed'
    UNION
    select account_id, month_id ,year_id ,transaction_requested_at ,transaction_completed_at ,amount, case when transfer_type = 'pix_in' then 'in' else 'out' end as transfer_type from (select
	account_id,
	pix_amount as amount,
	pix_requested_at as transaction_requested_at,
	pix_completed_at as transaction_completed_at,
	status,
	in_or_out as transfer_type,
	extract(month from action_timestamp) AS month_id ,
      extract(year from action_timestamp) AS year_id
from
	pix_movements as p
	left join d_time dt on p.pix_completed_at = dt.time_id
where
	status = 'completed'
) as pix_moviments
  ) AS transfer_data
  GROUP BY account_id, month_id, year_id
)
SELECT
  account_id,
  month_id,
  year_id,
  SUM(balance) OVER (PARTITION BY account_id ORDER BY year_id, month_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance
FROM transfer_balance;