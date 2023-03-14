import argparse
import s3fs

file_name = "dbt.duckdb"
bucket_name = "lbenninga-projects"
path = f"ticketswap/dbt/{file_name}"


def download_file(bucket_name, s3_source_path, local_path):
    fs = s3fs.S3FileSystem()
    with fs.open(f's3://{bucket_name}/{s3_source_path}', 'rb') as s3_file:
        with open(local_path, 'wb') as local_file:
            local_file.write(s3_file.read())
    print('File downloaded from S3 to local directory.')

def upload_file(bucket_name, local_path, s3_destination_path):
    fs = s3fs.S3FileSystem()
    with open(local_path, 'rb') as local_file:
        with fs.open(f's3://{bucket_name}/{s3_destination_path}', 'wb') as s3_file:
            s3_file.write(local_file.read())
    print('File uploaded from local directory to S3.')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download and upload files to/from Amazon S3.')
    subparsers = parser.add_subparsers(dest='command')

    # Download command
    download_parser = subparsers.add_parser('download', help='Download a file from S3 to local directory.')

    # Upload command
    upload_parser = subparsers.add_parser('upload', help='Upload a file from local directory to S3.')

    args = parser.parse_args()

    if args.command == 'download':
        download_file(bucket_name=bucket_name, s3_source_path=path, local_path=file_name)
    elif args.command == 'upload':
        upload_file(bucket_name=bucket_name, local_path=file_name, s3_destination_path=path)
    else:
        parser.print_help()
