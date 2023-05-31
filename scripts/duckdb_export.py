import duckdb
conn = duckdb.connect("dbt.duckdb")
conn.execute("EXPORT DATABASE 'tmp' (FORMAT PARQUET);")