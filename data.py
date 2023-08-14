import boto3
import pandas as pd
import sys
import os
from dotenv import load_dotenv

if sys.version_info[0] < 3: 
    from StringIO import StringIO # Python 2.x
else:
    from io import StringIO # Python 3.x

load_dotenv()

client = boto3.client('s3',
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID'), 
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY'))

bucket_name = 'disaster-message'

object_key = 'disaster_messages.csv'
csv_obj = client.get_object(Bucket=bucket_name, Key=object_key)
body = csv_obj['Body']
csv_string = body.read().decode('utf-8')

df = pd.read_csv(StringIO(csv_string))
print(df)
