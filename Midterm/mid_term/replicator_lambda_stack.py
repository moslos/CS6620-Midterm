from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_s3_notifications as s3_notifications,
    aws_dynamodb as dynamodb,
    aws_iam as iam
)
from constructs import Construct

class ReplicatorLambdaStack(Stack):

    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Import existing S3 buckets
        source_bucket = s3.Bucket.from_bucket_name(
            self, "BucketSrc", bucket_name="bucket-srcmidterm-zy"
        )
        destination_bucket = s3.Bucket.from_bucket_name(
            self, "BucketDst", bucket_name="bucket-dstmidterm-zy"
        )

        # Import existing DynamoDB table
        table = dynamodb.Table.from_table_name(
            self, "TableT", table_name="table-tmidterm-zy"
        )

        # Define the Replicator Lambda function
        replicator_lambda = _lambda.Function(
            self,
            "ReplicatorFunction",
            function_name="replicator-function",
            runtime=_lambda.Runtime.PYTHON_3_8,
            handler="replicator.lambda_handler",
            code=_lambda.Code.from_asset("lambdas/replicator.zip"),  # Adjust path as necessary
        )

        # Grant necessary permissions to Lambda function
        table.grant_read_write_data(replicator_lambda)
        source_bucket.grant_read(replicator_lambda)
        destination_bucket.grant_read_write(replicator_lambda)

        # Add S3 trigger for PUT and DELETE events
        notification = s3_notifications.LambdaDestination(replicator_lambda)
        source_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED, notification
        )
        source_bucket.add_event_notification(
            s3.EventType.OBJECT_REMOVED, notification
        )

        # Add additional permissions for the Lambda function
        replicator_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:ListBucket",
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject"
                ],
                resources=[
                    source_bucket.bucket_arn,
                    f"{source_bucket.bucket_arn}/*",
                    destination_bucket.bucket_arn,
                    f"{destination_bucket.bucket_arn}/*"
                ]
            )
        )

        replicator_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["dynamodb:Query"],
                resources=[
                    "arn:aws:dynamodb:us-east-1:586794441416:table/table-tmidterm-zy/index/DisownedIndex"
                ]
            )
        )
