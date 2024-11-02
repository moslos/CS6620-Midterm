import os
from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_s3_notifications as s3n,
    aws_s3 as s3,
)
from constructs import Construct

class ReplicatorStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, storage_stack, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # Define the Replicator Lambda function
        self.replicator_lambda = _lambda.Function(
            self,
            "ReplicatorFunction",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="replicator.handler",
            code=_lambda.Code.from_asset(os.path.join("lambda", "replicator")),
            environment={
                "BUCKET_SRC": storage_stack.bucket_src.bucket_name,
                "BUCKET_DST": storage_stack.bucket_dst.bucket_name,
                "TABLE_NAME": storage_stack.table_t.table_name,
            },
        )

        # Grant permissions to the Lambda function
        storage_stack.bucket_src.grant_read(self.replicator_lambda)
        storage_stack.bucket_dst.grant_write(self.replicator_lambda)
        storage_stack.table_t.grant_read_write_data(self.replicator_lambda)

        # Set up S3 event notifications for PUT and DELETE events
        notification = s3n.LambdaDestination(self.replicator_lambda)
        storage_stack.bucket_src.add_event_notification(
            s3.EventType.OBJECT_CREATED_PUT,
            notification
        )
        storage_stack.bucket_src.add_event_notification(
            s3.EventType.OBJECT_REMOVED_DELETE,
            notification
        )
