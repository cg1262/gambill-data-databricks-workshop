CREATE OR REFRESH STREAMING TABLE ARC
AS
SELECT sa.name AS account_name, SUM(so.amount) AS total_opportunity_amount
FROM STREAM silver_dev.sales.silver_opportunity so
INNER JOIN silver_dev.sales.silver_account sa ON so.account_id = sa.account_id
GROUP BY ALL;
