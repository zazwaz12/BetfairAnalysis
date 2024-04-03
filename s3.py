import boto3

def upload_file_to_s3(file_path, bucket_name, object_name):
    # Create an S3 client
    s3 = boto3.client('s3')

    # Upload the file to S3
    s3.upload_file(file_path, bucket_name, object_name)

# Replace these with your actual values
file_path = 'unprocessedmarkets.json'
bucket_name = 'betfair-json-bucket'
object_name = 'unprocessedmarkets.json'

upload_file_to_s3(file_path, bucket_name, object_name)
