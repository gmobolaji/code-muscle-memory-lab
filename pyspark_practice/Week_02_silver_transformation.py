Bronze = raw
Silver = cleaned + typed + conformed + enriched

so Silver work is mostly:

cleansing

deduping

type normalization

null imputation

standardization

enrichment

business rules


-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Before You Start: Reset Your Bronze Table
DROP TABLE IF EXISTS orders_raw;
DROP TABLE IF EXISTS orders_silver;

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#Bronze Data (Same for All Questions)
from pyspark.sql import Row

raw_data = [
    Row(order_id=1, customer_id=1001, order_date="2025-01-05", amount="100",    status="shipped"),
    Row(order_id=2, customer_id=1002, order_date="2025-01-06", amount="250",    status="Shipped"),
    Row(order_id=3, customer_id=1003, order_date="2025-01-07", amount="150",    status="SHIPPED"),
    Row(order_id=4, customer_id=1001, order_date="2025-01-05", amount="100",    status="shipped"),
    Row(order_id=5, customer_id=1004, order_date="2025-01-08", amount="INVALID",status="shipped"),
    Row(order_id=6, customer_id=1005, order_date="2025-01-09", amount="-50",    status="returned"),
    Row(order_id=7, customer_id=1006, order_date="2025-01-10", amount="500",    status="Delivered"),
    Row(order_id=8, customer_id=1007, order_date="2025-01-10", amount="450",    status="delivered"),
    Row(order_id=9, customer_id=1001, order_date="2025-01-05", amount="100",    status="SHIPPED"),
    Row(order_id=10, customer_id=1008, order_date="2025-01-11", amount="250",   status="PENDING")
]

bronze_df = spark.createDataFrame(raw_data)

bronze_df.write.format("delta").mode("overwrite").saveAsTable("orders_raw")

display(spark.table("orders_raw"))


-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#Question 1 — Standardize Status Column
# Goal: Convert all status values to uppercase.
from pyspark.sql.functions import upper, col

orders_raw = spark.table("orders_raw")

silver_df = orders_raw.withColumn("status_clean", upper(col("status")))

silver_df.write.format("delta").mode("overwrite").saveAsTable("orders_silver_status")

display(spark.table("orders_silver_status"))


-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#Question 2 — Convert Amount Column to Numeric
# Some amounts contain:"INVALID"
#negative values
#strings instead of numbers
# Goal:convert valid numbers
# turn invalid values into NULL
# This is classic Silver-zone cleansing.

from pyspark.sql.functions import when, col

orders_raw = spark.table("orders_raw")

silver_df = (
    orders_raw
        .withColumn("amount_clean",
            when(col("amount").rlike("^[0-9]+$"), col("amount").cast("double"))
            .otherwise(None)
        )
)

silver_df.write.format("delta").mode("overwrite").saveAsTable("orders_silver_amount")

display(spark.table("orders_silver_amount"))

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#Question 3 — Remove Duplicate Orders
# Duplicate = same (order_id, customer_id, order_date, amount)
orders_raw = spark.table("orders_raw")

silver_df = orders_raw.dropDuplicates(
    ["order_id", "customer_id", "order_date", "amount"]
)

silver_df.write.format("delta").mode("overwrite").saveAsTable("orders_silver_dedup")

display(spark.table("orders_silver_dedup"))

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#Question 4 — Add Derived Columns (Year, Month, Day)
# Classic Silver enhancement.
from pyspark.sql.functions import to_date, year, month, dayofmonth, col

orders_raw = spark.table("orders_raw")

silver_df = (
    orders_raw
        .withColumn("order_date_clean", to_date(col("order_date")))
        .withColumn("order_year", year(col("order_date_clean")))
        .withColumn("order_month", month(col("order_date_clean")))
        .withColumn("order_day", dayofmonth(col("order_date_clean")))
)

silver_df.write.format("delta").mode("overwrite").saveAsTable("orders_silver_dates")

display(spark.table("orders_silver_dates"))

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#Question 5 — Filter Out Invalid or Bad Records (Silver Curation)
# A Silver table should NOT contain:
# negative amounts
# NULL amounts
# bad dates
from pyspark.sql.functions import to_date, col

orders_raw = spark.table("orders_raw")

silver_df = (
    orders_raw
        # Convert date
        .withColumn("order_date_clean", to_date(col("order_date")))
        # Convert amount
        .withColumn("amount_clean", 
            when(col("amount").rlike("^[0-9]+$"), col("amount").cast("double"))
            .otherwise(None)
        )
        # Filter out bad records
        .filter(col("amount_clean").isNotNull())
        .filter(col("amount_clean") > 0)
        .filter(col("order_date_clean").isNotNull())
)

silver_df.write.format("delta").mode("overwrite").saveAsTable("orders_silver_clean")

display(spark.table("orders_silver_clean"))
