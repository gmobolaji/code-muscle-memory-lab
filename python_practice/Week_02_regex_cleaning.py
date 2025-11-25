Pandas = notebook / desktop brain
Spark = production / enterprise brain

If you clean 150k records → Pandas is great.

If you clean 150 million records → Pandas will die, Spark will laugh.

Regex in Pandas = compiled regex objects, fast

Regex in PySpark = distributed, slower per row, but infinite scalable
(run on multiple nodes)

It depends on the data size and purpose.
I typically prototype regex transformations in Pandas for rapid validation, then move to PySpark for scalable execution in a production pipeline, especially when writing Bronze, Silver, and Gold Delta layers.

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
##Python
import pandas as pd
import re

data = {
    "customer_id": [1,2,3,4,5,6,7,8],
    "customer_phone": [
        "(212) 555-7788",
        "212.555.9011",
        "2125553344",
        "+1 212 555 8822",
        "212-555-0033",
        "N/A",
        None,
        ""
    ]
}

pdf = pd.DataFrame(data)

# strip non-digits
pdf["clean"] = pdf["customer_phone"].astype(str).apply(lambda x: re.sub(r"\D", "", x))

# drop invalid rows
pdf = pdf[pdf["clean"].str.len() == 10]

# apply formatting
pdf["clean_formatted"] = pdf["clean"].apply(lambda x: f"{x[0:3]}-{x[3:6]}-{x[6:10]}")

print(pdf)


----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

##Pyspark
from pyspark.sql import Row
from pyspark.sql.functions import regexp_replace, col, length, substring, concat, lit

data = [
    Row(customer_id=1, customer_phone="(212) 555-7788"),
    Row(customer_id=2, customer_phone="212.555.9011"),
    Row(customer_id=3, customer_phone="2125553344"),
    Row(customer_id=4, customer_phone="+1 212 555 8822"),
    Row(customer_id=5, customer_phone="212-555-0033"),
    Row(customer_id=6, customer_phone="N/A"),
    Row(customer_id=7, customer_phone=None),
    Row(customer_id=8, customer_phone="")
]

df = spark.createDataFrame(data)

df_clean = (
    df.withColumn("digits", regexp_replace(col("customer_phone"), r"\D", ""))  # keep only digits
      .filter(length(col("digits")) == 10)                                     # ensure valid
      .withColumn(
          "clean_formatted",
          concat(
              substring(col("digits"), 1, 3),
              lit("-"),
              substring(col("digits"), 4, 3),
              lit("-"),
              substring(col("digits"), 7, 4)
          )
      )
)

df_clean.show(truncate=False)

