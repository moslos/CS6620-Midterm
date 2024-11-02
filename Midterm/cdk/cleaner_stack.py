import os
from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as targets,
)
from constructs import Construct

class CleanerStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, storage_stack, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # Define the Cleaner Lambda function
        self.cleaner_lambda = _lambda.Function(
            self,
            "CleanerFunction",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="cleaner.handler",
            code=_lambda.Code.from_asset(os.path.join("lambda", "cleaner")),
            environment={
                "BUCKET_DST": storage_stack.bucket_dst.bucket_name,
                "TABLE_NAME": storage_stack.table_t.table_name,
            },
        )

        # Grant permissions to the Lambda function
        storage_stack.bucket_dst.grant_delete(self.cleaner_lambda)
        storage_stack.table_t.grant_read_write_data(self.cleaner_lambda)

        # Set up periodic trigger every minute (EventBridge minimum interval)
        rule = events.Rule(
            self,
            "CleanerRule",
            schedule=events.Schedule.rate(Duration.minutes(1)),  # 设置为每1分钟触发一次
        )
        rule.add_target(targets.LambdaFunction(self.cleaner_lambda))
