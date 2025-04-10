from sf_session import session
from snowflake.snowpark.functions import col

df = session.table('tasty_bytes_sample_data.raw_pos.menu')

df_filtered = df.filter(col('ITEM_CATEGORY') == 'Beverage' )

df_filtered.write.mode("overwrite").save_as_table("tasty_bytes_sample_data.raw_pos.menu_filtered")
print("write completed!")
session.sql("select * from tasty_bytes_sample_data.raw_pos.menu_filtered").show()