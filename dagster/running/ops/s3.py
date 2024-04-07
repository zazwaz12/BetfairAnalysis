import boto3
from dagster import op

def upload_file_to_s3(file_path, bucket_name, object_name):
    # Create an S3 client
    s3 = boto3.client('s3')

    # Upload the file to S3
    s3.upload_file(file_path, bucket_name, object_name)
    with open(file_path, "w") as outfile:
        pass #clears file
      
@op
def json_to_s3():
    file_path1 = 'newStream.json'
    file_path2 = 'unprocessedmarkets.json'
    bucket_name = 'betfair-json-bucket'
    object_name1 = 'Streams/newStream.json'
    object_name2 = 'MatchInfos/unprocessedmarkets.json'

    upload_file_to_s3(file_path1, bucket_name, object_name1)
    upload_file_to_s3(file_path2, bucket_name, object_name2)