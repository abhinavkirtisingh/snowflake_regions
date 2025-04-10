import os
from sf_session import session
from snowflake.snowpark.functions import col

database = "tasty_bytes_sample_data"  # use your own database and schema
schema = "raw_pos"
view_name = "test_view"
df = session.table('tasty_bytes_sample_data.raw_pos.menu')
print(df.count())
df.filter(col("ITEM_CATEGORY") == "Beverage").show(5)
df.create_or_replace_view("tasty_bytes_sample_data.raw_pos.test_view")

print("view created!")