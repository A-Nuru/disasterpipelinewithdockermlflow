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

object_keys = ['disaster_messages.csv', 'disaster_categories.csv']

"""
Module to load, clean and save data
"""
def load_data(bucket_name, object_key):
    
    '''
    Args:
        bucket_name: Name of the S3 bucket that contains data
        objet_key: Name of the subfolder that contains data
    Returns:
        df: loaded dataframe 
    '''

    csv_obj = client.get_object(Bucket=bucket_name, Key=object_key)
    body = csv_obj['Body']
    csv_string = body.read().decode('utf-8')

    df = pd.read_csv(StringIO(csv_string))
    print(df)
    return df


def merge_data(object_keys):
    '''
    Args:
        objet_keys: List of names (strings) of the subfolder that contains data
    Returns:
        df: Merged dataframe 
    '''
    df_messages = load_data(bucket_name, object_keys[0])
    df_categories = load_data(bucket_name, object_keys[1])
    
    df = pd.merge(df_messages, df_categories, on='id')
    print(df)
    return df

def main():
    merge_data(object_keys)    

if __name__ == '__main__':
    main()
