
import os
import boto3
import gzip
import shutil
path = "/home/nigel/noaa_data/data"

files = []
# r=root, d=directories, f = files
for r, d, f in os.walk(path):
    for file in f:
        files.append(os.path.join(r, file))
out = open('files', 'w')
out.write(str(files))

for file in files:
    print(file)
    ft = file[:-3]
    print(ft)
    with gzip.open(file, 'rb') as f_in:
        with open(ft, 'wb') as f_out:
        	shutil.copyfileobj(f_in, f_out)	
            
    
    AWS_BUCKET_NAME = 'kt-climate'
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(AWS_BUCKET_NAME)
    path = ft
    data = open(ft).read() 
    bucket.put_object(
        ACL='public-read',
        ContentType='text/plain',
        Key=path,
        Body=data,
    )

    body = {
        "uploaded": "true",
        "bucket": AWS_BUCKET_NAME,
        "path": path,
    }

    os.remove(ft)

    