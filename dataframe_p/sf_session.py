from snowflake.snowpark import Session

config = {

    "ACCOUNT": "LNZNUFZ-YK66707",
    "USER" : "ABHINAVKS",
    "PASSWORD": "Abhinav@200700",
    "ROLE": "ACCOUNTADMIN",
    "DATABASE" : "tasty_bytes_sample_data",
    "SCHEMA" : "raw_pos"
    
}



try:
    session = Session.builder.configs(config).create()
    print("Session Created!!")

    # Fetch and print details about the session
    print("Warehouse:", session.get_current_warehouse())
    print("Database:", session.get_current_database())
    print("Role:", session.get_current_role())

except Exception as e:
    print("Error occurred while creating the session:", e)
