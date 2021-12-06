"""Glue Job to execute historical load."""
import sys

from awsglue.utils import getResolvedOptions
from redshift_module import pygresql_redshift_common as rs_common

args = getResolvedOptions(sys.argv, ["db", "db_creds"])

# read the arguments coming from blueprint

db = args["db"]
db_creds = args["db_creds"]


sql = "COPY prime.sales_order_pacing from 's3://test-prod-datalake-us-east-1-000000/output/prime/prime_sales_order_pacing_rs_0710/2021-11-02' iam_role 'arn:aws:iam::111111111111:role/test-prod-redshift-service-role' FORMAT AS PARQUET;"

con = rs_common.get_connection(db, db_creds)

# Running the sql query one by one from the list
#for each_query in sql_list:
rs_common.query(con, sql)