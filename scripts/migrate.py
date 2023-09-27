import duckdb
import pandas as pd
import os

con = duckdb.connect(database="dbt.duckdb")
table_names = (
    con.execute("SELECT name FROM sqlite_master WHERE type='table'")
    .fetchdf()["name"]
    .tolist()
)


for table in table_names:
    result = con.execute("SELECT * FROM {}".format(table))
    df = result.fetchdf()
    df.to_csv("data/{}.csv".format(table), index=False)


# list only csv files in data folder
csv_files = [f for f in os.listdir("data") if f.endswith(".csv")]

con2 = duckdb.connect(database="dbt_v2.duckdb")

df = con2.sql("SELECT * FROM duckdb_tables;").df()
print(df)

for f in csv_files:
    df = pd.read_csv(f)
    con2.register(f, df)
