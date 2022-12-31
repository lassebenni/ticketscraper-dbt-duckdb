import argparse
import pandas as pd

# Parse command line arguments
parser = argparse.ArgumentParser()
parser.add_argument("csv_file", help="the name of the CSV file to be converted to Parquet")
args = parser.parse_args()

# Read in the CSV file
df = pd.read_csv(args.csv_file)

# Convert the CSV file to a Parquet file with the same name
parquet_file = args.csv_file.replace(".csv", ".parquet")
df.to_parquet(parquet_file, index=False)