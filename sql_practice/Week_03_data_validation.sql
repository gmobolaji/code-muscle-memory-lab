Question type	Why it matters
Missing mandatory fields	blocks ingestion, corrupts facts
Duplicate business keys	breaks dimension relationships & facts
Negative values	breaks financial rollups
Outliers	impacts aggregates, forecasting
Stale data	signals ingestion failure or system issue

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
%sql
--Create Bronze Table
CREATE OR REPLACE TABLE orders_raw (
  order_id INT,
  customer_id INT,
  order_date DATE,
  amount DECIMAL(10,2),
  status STRING
);

INSERT INTO orders_raw VALUES
(1, 101, '2025-01-02', 500.00, 'SHIPPED'),
(2, 101, '2025-01-09', 500.00, 'SHIPPED'), -- duplicate customer+amount
(3, 105, '2025-02-10', NULL, 'SHIPPED'),  -- null value
(4, NULL, '2025-01-04', 100.00, 'NEW'),   -- missing customer
(5, 106, NULL, 300.00, 'NEW'),            -- missing date
(6, 107, '2025-02-10', -200.00, 'CANCELED'); -- negative value

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

%sql
--Q1 — Find all rows with missing mandatory fields (critical in ETL validation before Silver) 
--mandatory fields = customer_id, order_date, amount
SELECT *
FROM orders_raw
WHERE customer_id IS NULL
   OR order_date IS NULL
   OR amount IS NULL;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

%sql
--Q2 — Check for duplicate business keys (Business rule duplicates — NOT PK duplicates)
--Here: business key = customer_id + order_date

SELECT customer_id, order_date, COUNT(*)
FROM orders_raw
GROUP BY customer_id, order_date
HAVING COUNT(*) > 1;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

%sql
--Q3 — Ensure numeric values are never negative (happens a LOT in migrations)
SELECT *
FROM orders_raw
WHERE amount < 0;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

%sql
--Q4 — Detect outliers using statistical boundaries (data engineering QA)
--Example: flag transactions above 2 standard deviations
WITH stats AS (
    SELECT AVG(amount) AS avg_amt,
           STDDEV(amount) AS sd_amt
    FROM orders_raw
)
SELECT r.*
FROM orders_raw r
CROSS JOIN stats s
WHERE amount > s.avg_amt + (2 * s.sd_amt);

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

%sql
--Q5 — Identify stale records (i.e., “Old records that haven’t been updated recently”)
SELECT *
FROM orders_raw
WHERE order_date < date_sub(current_date(), 60);
