import boto3

def upload_file_to_s3(file_path, bucket_name, object_name):
    # Create an S3 client
    s3 = boto3.client('s3')

    # Upload the file to S3
    s3.upload_file(file_path, bucket_name, object_name)
    with open(file_path, "w") as outfile:
        pass #clears file
      

# Replace these with your actual values
file_path = 'newStream.json'
bucket_name = 'betfair-json-bucket'
object_name = 'Streams/newStream.json'

upload_file_to_s3(file_path, bucket_name, object_name)
