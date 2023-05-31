import:
	duckdb dbt.duckdb -c "IMPORT DATABASE 'tmp';"

upload-db:
	poetry run python scripts/load_s3_files.py upload "something"