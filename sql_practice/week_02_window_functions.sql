--STEP 1 — Create a sample table inside Databricks
CREATE OR REPLACE TABLE sales_demo AS
SELECT *
FROM VALUES
  (1, 'East', 100),
  (2, 'East', 250),
  (3, 'West', 300),
  (4, 'West', 200),
  (5, 'South', 150),
  (6, 'South', 275)
AS t(order_id, region, amount);

SELECT * FROM sales_demo;
----------------------------------------------------------------------------------------------------------------------------------------------------------------

--Q1 — Calculate the running total of amount ordered by order_id
--Goal: cumulative revenue trend
SELECT
    order_id,
    amount,
    SUM(amount) OVER (ORDER BY order_id) AS running_total
FROM sales_demo;
----------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Q2 — For each region, calculate:
-- running total grouped by region
SELECT
    region,
    order_id,
    amount,
    SUM(amount) OVER (
        PARTITION BY region
        ORDER BY order_id
    ) AS region_running_total
FROM sales_demo;
----------------------------------------------------------------------------------------------------------------------------------------------------------------

--Q3 — Calculate average order amount per region (windowed, not grouped)
--Goal: return avg on every row, not collapsed
SELECT
    region,
    order_id,
    amount,
    AVG(amount) OVER (PARTITION BY region) AS avg_amount_region
FROM sales_demo;

----------------------------------------------------------------------------------------------------------------------------------------------------------------

--Q4 — Rank order value within region
SELECT
    region,
    order_id,
    amount,
    RANK() OVER (
        PARTITION BY region
        ORDER BY amount DESC
    ) AS region_rank
FROM sales_demo;

----------------------------------------------------------------------------------------------------------------------------------------------------------------

--Q5 — Dense ranking company-wide
SELECT
    order_id,
    amount,
    DENSE_RANK() OVER (ORDER BY amount DESC) AS global_dense_rank
FROM sales_demo;

----------------------------------------------------------------------------------------------------------------------------------------------------------------

--Q6 — Show difference between each row and previous row
SELECT
    order_id,
    amount,
    LAG(amount,1) OVER (ORDER BY order_id) AS prev_amount,
    amount - LAG(amount,1) OVER (ORDER BY order_id) AS diff_from_prev
FROM sales_demo;

----------------------------------------------------------------------------------------------------------------------------------------------------------------

--Q7 — Show difference vs region-previous row
SELECT
    region,
    order_id,
    amount,
    LAG(amount,1) OVER (PARTITION BY region ORDER BY order_id) AS prev_regional_amount,
    amount - LAG(amount,1) OVER (PARTITION BY region ORDER BY order_id) AS delta_from_prev_region
FROM sales_demo;

