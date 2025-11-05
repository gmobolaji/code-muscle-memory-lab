| Layer         | Purpose                              | Typical Data Source                         | Schema Strategy                                  | Why                                                  |
| ------------- | ------------------------------------ | ------------------------------------------- | ------------------------------------------------ | ---------------------------------------------------- |
| 🥉 **Bronze** | Raw ingestion                        | External files (CSV, JSON, API dumps, logs) | ❌ *Schema-on-read* (optional schema or inferred) | Flexibility — data may be messy or inconsistent      |
| 🥈 **Silver** | Cleansed, structured, validated      | Bronze → transformations                    | ✅ **Explicit schema**                            | Enforces data types, constraints, and business logic |
| 🥇 **Gold**   | Aggregated, business-ready analytics | Silver → aggregations, joins                | ✅ **Explicit + Versioned schema**                | Required for BI, governance, and stable reporting    |








from pyspark.sql import SparkSession

data = [
  (1, '2025-01-01', 'Electronics', 3, 120.00),
  (2, '2025-01-01', 'Clothing', 1, 45.00),
  (3, '2025-01-02', 'Electronics', 2, 80.00),
  (4, '2025-01-02', 'Sports', 5, 200.00),
  (5, '2025-01-03', 'Clothing', 3, 135.00),
  (6, '2025-01-03', 'Electronics', 1, 40.00),
  (7, '2025-01-03', 'Sports', 2, 90.00)
]

columns = ["order_id", "order_date", "category", "quantity", "amount"]

spark_df = spark.createDataFrame(data, columns)
spark_df.createOrReplaceTempView("sales")

display(spark_df)

