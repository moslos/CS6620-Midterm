from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
    RemovalPolicy,
)
from constructs import Construct

class StorageStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # Source S3 bucket
        self.bucket_src = s3.Bucket(self, "BucketSrc")

        # Destination S3 bucket
        self.bucket_dst = s3.Bucket(self, "BucketDst")

        # DynamoDB Table T
        self.table_t = dynamodb.Table(
            self,
            "TableT",
            partition_key=dynamodb.Attribute(
                name="SourceObjectName", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="CopyTimestamp", type=dynamodb.AttributeType.NUMBER
            ),
            removal_policy=RemovalPolicy.DESTROY,  # NOT recommended for production code
        )

        # Add Global Secondary Index (GSI) for IsDisowned and DisownedTimestamp
        self.table_t.add_global_secondary_index(
            index_name="DisownedIndex",
            partition_key=dynamodb.Attribute(
                name="IsDisowned", type=dynamodb.AttributeType.STRING  # Use string 'true'/'false'
            ),
            sort_key=dynamodb.Attribute(
                name="DisownedTimestamp", type=dynamodb.AttributeType.NUMBER
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )
