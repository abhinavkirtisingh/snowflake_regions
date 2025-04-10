from sf_session import session
from snowflake.snowpark.functions import col

df = session.table('tasty_bytes_sample_data.raw_pos.menu')
print(df.count())
df.filter(col("ITEM_CATEGORY") == "Beverage").show(5)
