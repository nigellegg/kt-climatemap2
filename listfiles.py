import boto3

_BUCKET_NAME = 'mybucket'
_PREFIX = 'subfolder/'

client = boto3.client('s3')

def ListFiles(client):
    """List files in specific S3 URL"""
    response = client.list_objects(Bucket=_BUCKET_NAME, Prefix=_PREFIX)
    for content in response.get('Contents', []):
        yield content.get('Key')

file_list = ListFiles(client)
for file in file_list:
    print('File found: %s' % file)
