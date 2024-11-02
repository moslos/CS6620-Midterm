from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
)
from constructs import Construct

class CleanerStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        # DynamoDB table and destination bucket names
        table_name = "table-tmidterm-zy"
        destination_bucket_name = "bucket-dstmidterm-zy"

        # Reference to the existing DynamoDB table
        table = dynamodb.Table.from_table_name(self, 'TableT', table_name=table_name)

        # Reference to the existing destination S3 bucket
        dest_bucket = s3.Bucket.from_bucket_name(self, 'DestBucket', bucket_name=destination_bucket_name)

        # Define the Cleaner Lambda function
        cleaner_lambda = _lambda.Function(
            self, 'CleanerLambda',
            runtime=_lambda.Runtime.PYTHON_3_8,
            handler='cleaner.lambda_handler',
            function_name="cleaner-function",
            code=_lambda.Code.from_asset("lambdas/cleaner.zip"),  # Adjust path if needed
            timeout=Duration.seconds(300),
        )

        # Grant permissions to Cleaner Lambda for DynamoDB and S3
        table.grant_read_write_data(cleaner_lambda)
        dest_bucket.grant_delete(cleaner_lambda)

        # Add IAM policy for scanning and managing DynamoDB and S3
        cleaner_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "dynamodb:Scan",
                    "dynamodb:Query",
                    "dynamodb:DeleteItem",
                    "s3:DeleteObject",
                ],
                resources=[
                    table.table_arn,
                    f"{table.table_arn}/index/DisownedIndex"  # Include access to the GSI
                ],
            )
        )

        # Define an EventBridge rule for periodic triggering of the Cleaner Lambda
        rule = events.Rule(
            self, "CleanerLambdaSchedule",
            schedule=events.Schedule.rate(Duration.minutes(1)),  # Set the trigger rate
        )

        # Add Cleaner Lambda as the target for the EventBridge rule
        rule.add_target(targets.LambdaFunction(cleaner_lambda))
