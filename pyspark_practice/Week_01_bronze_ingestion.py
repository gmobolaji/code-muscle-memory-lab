| Layer         | Purpose                              | Typical Data Source                         | Schema Strategy                                  | Why                                                  |
| ------------- | ------------------------------------ | ------------------------------------------- | ------------------------------------------------ | ---------------------------------------------------- |
| 🥉 **Bronze** | Raw ingestion                        | External files (CSV, JSON, API dumps, logs) | ❌ *Schema-on-read* (optional schema or inferred) | Flexibility — data may be messy or inconsistent      |
| 🥈 **Silver** | Cleansed, structured, validated      | Bronze → transformations                    | ✅ **Explicit schema**                            | Enforces data types, constraints, and business logic |
| 🥇 **Gold**   | Aggregated, business-ready analytics | Silver → aggregations, joins                | ✅ **Explicit + Versioned schema**                | Required for BI, governance, and stable reporting    |


If you’re landing raw, unvalidated data into a Delta table with zero business rules applied, that is Bronze ingestion.
🟫 What defines Bronze?

A Bronze table is:

✔ raw
✔ minimally processed
✔ 1:1 with source
✔ appended
✔ schema may be messy
✔ nulls allowed
✔ no cleansing
✔ no validation
✔ no business rules
✔ no joins

Bronze = Raw landing zone
Think: drop-zone

Bronze’s job is to capture source exactly as-is, reliably, not perfectly.
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#Q1 — Create a Delta table from a DataFrame
#Goal: create a Delta table named sales_bronze
from pyspark.sql import Row
from pyspark.sql.functions import col

# Bronze
data = [
    Row(order_id=1, amount=100),
    Row(order_id=2, amount=250),
    Row(order_id=3, amount=150)
]

df = spark.createDataFrame(data)

df.write.format("delta").mode("overwrite").saveAsTable("sales_bronze")

spark.table("sales_bronze").show()
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Q2 — Read that Delta table and transform it (Silver)
# Goal: create a curated table
silver_df = (
    spark.table("sales_bronze")
         .withColumn("amount_usd", col("amount") * 1.00)
)

silver_df.write.format("delta").mode("overwrite").saveAsTable("sales_silver")

spark.table("sales_silver").show()
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Q3 — Run a Delta UPDATE
# Goal: add discount to all orders above 200
spark.sql("""
UPDATE sales_silver
SET amount_usd = amount_usd * 0.9
WHERE amount_usd > 200
""")
spark.sql("SELECT * FROM sales_silver").show()

------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Q4 — Run a Delta MERGE (SCD-like)
# Goal: merge new orders into Silver
# Incoming DF
new_data = [
    (2, 300),   # updated amount
    (4, 500)    # new order
]

incoming_df = spark.createDataFrame(new_data, ["order_id", "amount"])
incoming_df.createOrReplaceTempView("incoming_updates")

# Merge Script
spark.sql("""
MERGE INTO sales_silver tgt
USING incoming_updates src
ON tgt.order_id = src.order_id
WHEN MATCHED THEN UPDATE SET tgt.amount_usd = src.amount
WHEN NOT MATCHED THEN INSERT (order_id, amount_usd) VALUES (src.order_id, src.amount)
""")


spark.sql("SELECT * FROM sales_silver ORDER BY order_id").show()

------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Q5 — Create a Gold aggregate table
spark.sql("""
CREATE OR REPLACE TABLE sales_gold AS
SELECT
    AVG(amount_usd) AS avg_amount,
    SUM(amount_usd) AS total_amount
FROM sales_silver
""")

spark.sql("SELECT * FROM sales_gold").show()
