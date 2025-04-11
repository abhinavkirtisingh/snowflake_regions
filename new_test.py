import os
from datetime import timedelta
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.connector import connect
from sf_session import session
from snowflake.snowpark import functions as F

from snowflake.core import Root
from snowflake.core.task import StoredProcedureCall, Task
from snowflake.core.task.dagv1 import DAGOperation, DAG, DAGTask

from dotenv import load_dotenv
load_dotenv('.env')
