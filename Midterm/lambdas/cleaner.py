import boto3
import time

# Initialize AWS clients
dynamo_resource = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')
s3_resource = boto3.resource('s3')
dynamo_client = boto3.client('dynamodb')

# Environment variables
DYNAMO_TABLE_NAME = "table-tmidterm-zy"
DEST_BUCKET_NAME = "bucket-dstmidterm-zy"
LAMBDA_ARN_SELF_INVOKE = "arn:aws:lambda:us-east-1:586794441416:function:cleaner-function"

# DynamoDB table reference
dynamo_table = dynamo_resource.Table(DYNAMO_TABLE_NAME)

def lambda_handler(event, context):
    current_time = int(time.time())
    cutoff_time = current_time - 60  # 60 seconds ago

    print(f"Lambda execution started at timestamp: {current_time}, cutoff time: {cutoff_time}")

    # Query DynamoDB for disowned items older than 60 seconds
    try:
        print("Querying DynamoDB for disowned items...")
        query_response = dynamo_client.query(
            TableName=DYNAMO_TABLE_NAME,
            IndexName='DisownedIndex',
            KeyConditionExpression='#is_disowned = :disowned AND #disowned_at <= :cutoff',
            ExpressionAttributeNames={
                '#is_disowned': 'IsDisownedFlag',
                '#disowned_at': 'DisownedAt'
            },
            ExpressionAttributeValues={
                ':disowned': {'S': 'Y'},
                ':cutoff': {'N': str(cutoff_time)}
            }
        )
        print("Query response received:", query_response)
    except Exception as query_error:
        print(f"Error querying DynamoDB: {query_error}")
        return

    disowned_items = query_response.get('Items', [])
    print(f"Total disowned items to process: {len(disowned_items)}")

    # Process each disowned item
    for item in disowned_items:
        bucket_name = item['BucketName']['S']
        source_object_key = item['SourceObjectName']['S']
        destination_object_key = item['CopyObjectName']['S']
        creation_timestamp = int(item['CopyCreationTimestamp']['N'])

        print(f"Processing item: Bucket='{bucket_name}', Source='{source_object_key}', "
              f"Destination='{destination_object_key}', Timestamp='{creation_timestamp}'")

        # Delete the copy from the destination bucket
        try:
            s3_client.delete_object(Bucket=DEST_BUCKET_NAME, Key=destination_object_key)
            print(f"Deleted object from destination bucket: {destination_object_key}")
        except Exception as delete_error:
            print(f"Error deleting object '{destination_object_key}' from destination bucket: {delete_error}")

        # Delete the item from DynamoDB
        try:
            dynamo_table.delete_item(
                Key={
                    'BucketName': bucket_name,
                    'CopyCreationTimestamp': creation_timestamp
                }
            )
            print(f"Deleted DynamoDB item: Bucket='{bucket_name}', Timestamp='{creation_timestamp}'")
        except Exception as delete_item_error:
            print(f"Error deleting DynamoDB item: {delete_item_error}")

    print("Lambda execution completed.")
