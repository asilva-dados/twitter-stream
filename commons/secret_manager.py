import os
import json
import boto3

def get_secret_key(secret_name: str, region: str) -> str:
    access_key = None
    aws_access_key = os.environ['AWS_ACCESS_KEY']
    aws_access_secret_key = os.environ['AWS_SECRET_KEY']     
    client = boto3.client(service_name='secretsmanager', 
                          region_name=region, 
                          aws_access_key_id=aws_access_key, 
                          aws_secret_access_key=aws_access_secret_key)
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(get_secret_value_response['SecretString'].strip())
    access_key['CONSUMER_KEY'] = secret['CONSUMER_KEY'].strip()
    access_key['CONSUMER_SECRET'] = secret['CONSUMER_SECRET'].strip()
    access_key['ACCESS_TOKEN'] = secret['ACCESS_TOKEN'].strip()
    access_key['ACCESS_TOKEN_SECRET'] = secret['ACCESS_TOKEN_SECRET'].strip()
    access_key['BEARER_TOKEN'] = secret['BEARER_TOKEN'].strip()
    return access_key
