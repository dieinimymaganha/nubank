WITH movements AS (
	SELECT
		transfer_ins.account_id AS account_id,
		d_time.month_id AS month_id,
		d_time.year_id AS year_id,
		'in' AS in_or_out,
		sum(transfer_ins.amount) AS sum
	FROM
		transfer_ins
		LEFT JOIN d_time ON transfer_ins.transaction_completed_at = d_time.time_id
	WHERE
		transfer_ins.status = 'completed'
	GROUP BY
		transfer_ins.account_id,
		d_time.month_id,
		d_time.year_id
	UNION
	ALL
	SELECT
		transfer_outs.account_id AS account_id,
		d_time.month_id AS month_id,
		d_time.year_id AS year_id,
		'out' AS in_or_out,
		sum(transfer_outs.amount) AS sum
	FROM
		transfer_outs
		LEFT JOIN d_time ON transfer_outs.transaction_completed_at = d_time.time_id
	WHERE
		transfer_outs.status = 'completed'
	GROUP BY
		transfer_outs.account_id,
		d_time.month_id,
		d_time.year_id
	UNION
	ALL
	SELECT
		pix_movements.account_id AS account_id,
		d_time.month_id AS month_id,
		d_time.year_id AS year_id,
		pix_movements.in_or_out AS in_or_out,
		sum(pix_movements.pix_amount) AS sum
	FROM
		pix_movements
		LEFT JOIN d_time ON pix_movements.pix_completed_at = d_time.time_id
	WHERE
		pix_movements.status = 'completed'
	GROUP BY
		pix_movements.account_id,
		d_time.month_id,
		d_time.year_id,
		pix_movements.in_or_out
)
SELECT
	account_id,
	month_id,
	year_id,
	SUM(
		CASE
			WHEN in_or_out = 'in' THEN sum
			ELSE - sum
		END
	) AS balance
FROM
	movements
GROUP BY
	account_id,
	month_id,
	year_id
ORDER BY
	account_id ASC,
	year_id ASC,
	month_id ASC;