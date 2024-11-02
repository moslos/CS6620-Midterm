import boto3
import time
from urllib.parse import unquote_plus
from boto3.dynamodb.conditions import Attr
from boto3.dynamodb.conditions import Key



# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')

# Environment variables
TABLE_NAME = "table-tmidterm-zy"
DESTINATION_BUCKET = "bucket-dstmidterm-zy"

# DynamoDB table reference
table = dynamodb.Table(TABLE_NAME)

def lambda_handler(event, context):
    for record in event['Records']:
        event_name = record['eventName']
        bucket_name = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        if event_name.startswith('ObjectCreated'):
            handle_put_event(bucket_name, key)
        else:
            handle_delete_event(bucket_name, key)

def handle_put_event(bucket_name, source_object_key):
    current_timestamp = int(time.time())
    new_copy_object_name = f"{source_object_key}"
    copy_source = {'Bucket': bucket_name, 'Key': source_object_key}

    s3_client.copy_object(
        Bucket=DESTINATION_BUCKET,
        CopySource=copy_source,
        Key=new_copy_object_name
    )

    response = table.query(
        IndexName="DisownedIndex",
        KeyConditionExpression=Key('IsDisownedFlag').eq("N") & Key('DisownedAt').eq(0),
        FilterExpression=Attr('SourceObjectName').eq(source_object_key)
    )

    if response['Items']:
        item = response['Items'][0]
        timestamp = item['CopyCreationTimestamp']
        bucket_name = item['BucketName']
        table.delete_item(
            Key={
                'BucketName': bucket_name,
                'CopyCreationTimestamp': timestamp
            }
        )

    table.put_item(
        Item={
            'BucketName': bucket_name,
            'SourceObjectName': source_object_key,
            'CopyObjectName': new_copy_object_name,
            'CopyCreationTimestamp': current_timestamp,
            'IsDisownedFlag': 'N',
            'DisownedAt': 0
        }
    )

def handle_delete_event(bucket_name, source_object_key):
    current_timestamp = int(time.time())
    response = table.query(
        IndexName="DisownedIndex",
        KeyConditionExpression=Key('IsDisownedFlag').eq("N") & Key('DisownedAt').eq(0),
        FilterExpression=Attr('SourceObjectName').eq(source_object_key)
    )

    if response['Items']:
        item = response['Items'][0]
        timestamp = item['CopyCreationTimestamp']
        bucket_name = item['BucketName']
        table.update_item(
            Key={
                'BucketName': bucket_name,
                'CopyCreationTimestamp': timestamp
            },
            UpdateExpression="SET IsDisownedFlag = :disowned, DisownedAt = :timestamp",
            ExpressionAttributeValues={
                ':disowned': 'Y',
                ':timestamp': current_timestamp
            }
        )
