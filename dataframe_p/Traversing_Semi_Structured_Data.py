# Traversing_Semi-Structured_Data.py
from sf_session import session
from snowflake.snowpark.functions import col, get, get_path, lit
from snowflake.snowpark.types import *

df = session.table("tasty_bytes_sample_data.raw_pos.car_sales")
df.show()

df_new = df.select(col("src")["salesperson"]["name"], col("src")["salesperson"]["id"])
df_new.show()
df.select(df["src"]["vehicle"][0]).show()
df.select(df["src"]["vehicle"][0]["price"]).show()

df.show()
df.select(get(col("src"), lit("dealership"))).show()

df.select(get_path(col("src"), lit("vehicle[0].make"))).show()
df.select(col("src")["vehicle"][0]["make"]).show()

df.printSchema()

# Explicitly Casting Values in Semi-Structured Data
# By default, the values of fields and elements are returned as string literals (including the double quotes), as shown in the examples above.

# To avoid unexpected results, call the cast method to cast the value to a specific type. For example, the following code prints out the values 
# without and with casting:
df.select(col("src")["salesperson"]["id"]).show()
df.select(col("src")["salesperson"]["id"].cast(IntegerType())).show()