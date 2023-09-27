import duckdb

local_con = duckdb.connect("md:")
local_con.sql("CREATE OR REPLACE DATABASE motherduck_dbt FROM 'dbt.duckdb'")
