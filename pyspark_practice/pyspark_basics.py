from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count

spark = SparkSession.builder.appName("Week1-Basics").getOrCreate()

# 1. Load CSV
df = spark.read.option("header", True).csv("data/employees.csv")

# 2. Cleanse and Transform
df_clean = df.withColumn("salary", col("salary").cast("double")) \
             .withColumn("level", when(col("salary") < 50000, "Entry")
                                     .when(col("salary") < 100000, "Mid")
                                     .otherwise("Senior"))

# 3. Aggregate Example
df_summary = df_clean.groupBy("level").agg(count("*").alias("employee_count"))
df_summary.show()
