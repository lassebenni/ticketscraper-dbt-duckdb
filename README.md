## Ticketscraper duckdb DBT

Project


Steps
1. Read full ticketprices history with git-history and store as a CSV.
2. Convert history CSV into Parquet.
3. Store Parquet in S3.
4. Add credentials to project to read S3 history parquet file.
5. Use to build project, create incremental table parquet file.
6. Add code to ticketscraper to store incremental onto S3 as parquet (new file for each day).
7. Modify dbt project to create and read the duck.db file from S3 and to read incremental parquet files from S3. 
8. Create dbt model and snapshot parquet file on S3.
9. Read S3 parquet models in Rill and create models + dashboards.
10. Create Dockerfile for RIll to run on Cloud Run.
11. Run Rill in browser, automatically getting latest updates for the new files in S3.