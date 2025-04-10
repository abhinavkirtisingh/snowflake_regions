# Flattening_an_Array_of_Objects_into_Rows.py
from sf_session import session
from snowflake.snowpark.functions import col, get, get_path, lit
from snowflake.snowpark.types import *

df = session.table("tasty_bytes_sample_data.raw_pos.car_sales")
df.join_table_function("flatten", col("src")["customer"]).show()

df.join_table_function("flatten", col("src")["customer"]).select(col("value")["name"], col("value")["address"]).show()

df.join_table_function("flatten", col("src")["customer"]).select(col("value")["name"].cast(StringType()).as_("Customer Name"), col("value")["address"].cast(StringType()).as_("Customer Address")).show()