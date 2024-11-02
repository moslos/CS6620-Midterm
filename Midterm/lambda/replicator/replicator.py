import os
import boto3
import json
import time
from boto3.dynamodb.conditions import Key

# Initialize AWS clients
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['TABLE_NAME'])
bucket_src_name = os.environ['BUCKET_SRC']
bucket_dst_name = os.environ['BUCKET_DST']

def handler(event, context):
    print("Received event:", json.dumps(event))
    for record in event['Records']:
        event_name = record['eventName']
        s3_object = record['s3']
        key = s3_object['object']['key']

        source_object_name = key
        current_timestamp = int(time.time())

        if event_name.startswith('ObjectCreated:'):
            # Handle PUT event
            copy_object_name = f"{source_object_name}-{current_timestamp}"
            copy_source = {
                'Bucket': bucket_src_name,
                'Key': source_object_name
            }
            # Copy object to Bucket Dst
            s3.copy_object(
                Bucket=bucket_dst_name,
                Key=copy_object_name,
                CopySource=copy_source
            )
            # Insert new item into Table T
            item = {
                'SourceObjectName': source_object_name,
                'CopyTimestamp': current_timestamp,
                'CopyObjectName': copy_object_name,
                'IsDisowned': 'false',
            }
            table.put_item(Item=item)
            # Query existing copies
            response = table.query(
                KeyConditionExpression=Key('SourceObjectName').eq(source_object_name),
                ScanIndexForward=True  # Ascending order by CopyTimestamp
            )
            items = response.get('Items', [])
            if len(items) > 1:
                # Delete the oldest copy
                oldest_item = items[0]
                oldest_copy_object_name = oldest_item['CopyObjectName']
                # Delete from Bucket Dst
                s3.delete_object(
                    Bucket=bucket_dst_name,
                    Key=oldest_copy_object_name
                )
                # Delete from Table T
                table.delete_item(
                    Key={
                        'SourceObjectName': oldest_item['SourceObjectName'],
                        'CopyTimestamp': oldest_item['CopyTimestamp']
                    }
                )
        elif event_name.startswith('ObjectRemoved:'):
            # Handle DELETE event
            response = table.query(
                KeyConditionExpression=Key('SourceObjectName').eq(source_object_name)
            )
            items = response.get('Items', [])
            for item in items:
                # Update IsDisowned and DisownedTimestamp
                table.update_item(
                    Key={
                        'SourceObjectName': item['SourceObjectName'],
                        'CopyTimestamp': item['CopyTimestamp']
                    },
                    UpdateExpression="SET IsDisowned = :d, DisownedTimestamp = :t",
                    ExpressionAttributeValues={
                        ':d': 'true',
                        ':t': current_timestamp
                    }
                )
        else:
            print(f"Unhandled event type: {event_name}")
