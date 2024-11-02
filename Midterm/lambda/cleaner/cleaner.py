import os
import boto3
import time
from boto3.dynamodb.conditions import Key

# Initialize AWS clients
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['TABLE_NAME'])
bucket_dst_name = os.environ['BUCKET_DST']

def handler(event, context):
    print("Cleaner Lambda triggered")
    current_timestamp = int(time.time())
    cutoff_timestamp = current_timestamp - 10  # Items disowned for more than 10s

    # Query the GSI for disowned copies older than 10s
    response = table.query(
        IndexName='DisownedIndex',
        KeyConditionExpression=Key('IsDisowned').eq('true') & Key('DisownedTimestamp').lte(cutoff_timestamp)
    )
    items = response.get('Items', [])
    for item in items:
        copy_object_name = item['CopyObjectName']
        # Delete the copy from Bucket Dst
        s3.delete_object(
            Bucket=bucket_dst_name,
            Key=copy_object_name
        )
        # Delete the item from Table T
        table.delete_item(
            Key={
                'SourceObjectName': item['SourceObjectName'],
                'CopyTimestamp': item['CopyTimestamp']
            }
        )
