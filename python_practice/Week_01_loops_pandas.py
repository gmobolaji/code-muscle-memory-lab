## 🧠 Loops in PySpark vs Pandas — Key Concepts, Examples & Gotchas

### 📘 Overview

This notebook compares how **iteration and row-level logic** are handled in **Pandas** (local, in-memory operations) and **PySpark** (distributed, parallelized operations).
While both can achieve similar results, the **execution model**, **performance**, and **syntax** differ significantly.

### ⚡ Core Concept

| Feature        | **Pandas**                                  | **PySpark**                             |
| -------------- | ------------------------------------------- | --------------------------------------- |
| Execution      | Local, in-memory                            | Distributed, lazy execution             |
| Scale          | Small to medium datasets                    | Very large datasets                     |
| Loops          | Explicit (e.g., `for row in df.iterrows()`) | Avoided (use transformations instead)   |
| Performance    | Fast for small data                         | Optimized for large data via Spark jobs |
| DataFrame Type | `pandas.DataFrame`                          | `pyspark.sql.DataFrame`                 |

---

### 🧩 1. Looping in **Pandas**

#### Example

```python
import pandas as pd

data = {
    "region": ["East", "West", "East", "North"],
    "value": [10, 20, 30, 40]
}
pandas_df = pd.DataFrame(data)

# Example: Categorize values using a Python loop
for index, row in pandas_df.iterrows():
    if row["value"] > 20:
        pandas_df.loc[index, "value_tier"] = "High"
    else:
        pandas_df.loc[index, "value_tier"] = "Low"

display(pandas_df)
```

#### ✅ Works fine for:

* Exploratory data work
* Small datasets
* Quick transformations

#### ⚠️ Gotchas

* **Slow** for large data (loops are row-by-row).
* `iterrows()` returns copies, not views — assigning inside may not persist changes unless `.loc` is used.
* Consumes **a lot of memory** if dataset > few hundred MB.

---

### 🚀 2. Equivalent Logic in **PySpark**

#### Example

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

spark = SparkSession.builder.getOrCreate()

data = [("East", 10), ("West", 20), ("East", 30), ("North", 40)]
columns = ["region", "value"]
spark_df = spark.createDataFrame(data, columns)

# Transform using vectorized functions (no explicit loops)
spark_df = spark_df.withColumn(
    "value_tier",
    when(col("value") > 20, "High").otherwise("Low")
)

display(spark_df)
```

#### ✅ Works best for:

* Any data volume (scales across nodes)
* Production-grade ETL
* Complex conditional transformations

#### ⚠️ Gotchas

* **Don’t use `collect()` or loops in PySpark** — it forces all data into driver memory.
* Transformations are **lazy** — Spark doesn’t execute until an action (`.show()`, `.count()`, `.write()`) is called.
* Functions must be **expressed as transformations**, not standard Python loops.

---

### 🔁 3. Why “Loops” Differ

| Behavior             | Pandas                           | PySpark                                |
| -------------------- | -------------------------------- | -------------------------------------- |
| Iteration style      | Explicit (Python `for`)          | Implicit (Spark transformations)       |
| Memory usage         | All in driver                    | Distributed across executors           |
| Function application | `apply()`, `map()`, `iterrows()` | `withColumn()`, `when()`, `udf()`      |
| Common pitfall       | Forgetting `.loc` when assigning | Using loops or `collect()` incorrectly |

---

### ⚙️ 4. Common Transition Gotchas

| Mistake                               | Why It Fails                                   | Correct PySpark Fix                |
| ------------------------------------- | ---------------------------------------------- | ---------------------------------- |
| `for row in spark_df.collect()`       | Pulls entire dataset to driver                 | Use `.withColumn()` or `.filter()` |
| `spark_df['col'] = ...`               | Spark DataFrames are immutable                 | Use `.withColumn("col", expr)`     |
| Applying Python function directly     | Spark can’t parallelize plain Python functions | Register as UDF (`pandas_udf`)     |
| Expecting `print()` to show DataFrame | Spark uses lazy eval                           | Use `.show()` or `display()`       |

---

### Pro Tip — Think *Declaratively*, Not *Iteratively*

Instead of looping over data, **declare transformations**:

```python
# Pandas mindset
for row in df: ...
df.apply(lambda ...)

# PySpark mindset
df = df.withColumn("new_col", expr("CASE WHEN ... THEN ... END"))
```

---

### 5. Performance Comparison (Example)

| Operation                 | Pandas (10M rows) | PySpark (10M rows)      |
| ------------------------- | ----------------- | ----------------------- |
| Row loop                  | ❌ Crashes / slow  | ❌ Incorrect approach    |
| Vectorized `.apply()`     | ⚠️ 30–40 sec      | ❌ Unsupported           |
| `withColumn()` + `when()` | ❌ N/A             | ✅ 4–6 sec (distributed) |

---

### 🧭 6. Summary

* **Use Pandas** for data exploration, prototyping, or small-scale local tasks.
* **Use PySpark** for distributed ETL, orchestration, or large-scale transformations.
* Avoid loops in Spark — instead, think in terms of **transformations**, **UDFs**, and **actions**.
* Always **display()** instead of `print()` in Databricks notebooks.

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


#Step 1: Create the dataset in PySpark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

spark = SparkSession.builder.appName("PandasVsPySparkLoops").getOrCreate()

data = [
    (1001, "John Doe", "Electronics", 2, 500.0, "East"),
    (1002, "Jane Smith", "Home", 1, 150.0, "West"),
    (1003, "Bob Lee", "Electronics", 5, 100.0, "East"),
    (1004, "Alice Wong", "Furniture", 3, 300.0, "North"),
    (1005, "Sam Patel", "Home", 2, 150.0, "West"),
    (1006, "Linda Gray", "Furniture", 1, 450.0, "South")
]

schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer", StringType(), True),
    StructField("category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("region", StringType(), True)
])

df = spark.createDataFrame(data, schema)
df.show()

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#convert it to Pandas to practice both sides
pandas_df = df.toPandas()
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#pyspark
# 1 Add a derived column using conditional logic
# Tag each row as “Bulk” if quantity ≥ 3, else “Regular”.
from pyspark.sql.functions import when, col

df = df.withColumn("order_type", when(col("quantity") >= 3, "Bulk").otherwise("Regular"))
df.show()

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#pandas
# 1 Add a derived column using conditional logic
# Tag each row as “Bulk” if quantity ≥ 3, else “Regular”.
for i, row in pandas_df.iterrows():
    if row["quantity"] >= 3:
        pandas_df.at[i, "order_type"] = "Bulk"
    else:
        pandas_df.at[i, "order_type"] = "Regular"
display(pandas_df)

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#Pandas
# 2 Compute total order value
# Calculate total_value = quantity * price.
for i, row in pandas_df.iterrows():
    pandas_df.at[i, "total_value"] = row["quantity"] * row["price"]
display(pandas_df)

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#Pyspark
# 2 Compute total order value
# Calculate total_value = quantity * price
df = df.withColumn("total_value", col("quantity") * col("price"))
df.show()

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#Pandas
# 3 Classify total_value
# Question: Add a column value_tier: “High” if > 1000 “Medium” if 500–1000 “Low” otherwise
for i, row in pandas_df.iterrows():
    if row["total_value"] > 1000:
        pandas_df.at[i, "value_tier"] = "High"
    elif row["total_value"] >= 500:
        pandas_df.at[i, "value_tier"] = "Medium"
    else:
        pandas_df.at[i, "value_tier"] = "Low"
display(pandas_df)

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#Pyspark
# 3 Classify total_value
# Question: Add a column value_tier: “High” if > 1000 “Medium” if 500–1000 “Low” otherwise
df = df.withColumn(
    "value_tier",
    when(col("total_value") > 1000, "High")
    .when(col("total_value") >= 500, "Medium")
    .otherwise("Low")
)
df.show()

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#Pandas
# 4 Loop filter
# Question: Get all rows where region == "East" and value_tier == "High".
import pandas

filtered_rows = []
for i, row in pandas_df.iterrows():
    if row["region"] == "East" and row["value_tier"] == "High":
        filtered_rows.append(row)

result = pandas.DataFrame(filtered_rows)
display(pandas_df)

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#Pyspark
# 4 Loop filter
# Question: Get all rows where region == "East" and value_tier == "High".
df = df.withColumn(
    "value_tier",
    when(col("total_value") > 1000, "High")
    .when(col("total_value") >= 500, "Medium")
    .otherwise("Low")
)
df.show()

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#Pandas
#5 Aggregate results (instead of looping to sum)
# Question:Get total revenue and order count by region.
summary = {}
for i, row in pandas_df.iterrows():
    region = row["region"]
    if region not in summary:
        summary[region] = {"revenue": 0, "orders": 0}
    summary[region]["revenue"] += row["total_value"]
    summary[region]["orders"] += 1
import pandas as pd
pd.DataFrame.from_dict(summary, orient="index")

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#Pyspark
#5 Aggregate results (instead of looping to sum)
# Question:Get total revenue and order count by region.

from pyspark.sql.functions import sum as _sum, count

df.groupBy("region").agg(_sum("total_value").alias("revenue"), count("*").alias("orders")).show()

