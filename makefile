import:
	duckdb dbt.duckdb -c "IMPORT DATABASE 'tmp';"

download-db:
	poetry run python scripts/load_s3_files.py download

upload-db:
	poetry run python scripts/load_s3_files.py upload "something"