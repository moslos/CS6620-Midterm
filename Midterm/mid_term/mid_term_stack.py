from aws_cdk import (
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
    RemovalPolicy,
    Stack,
)
from constructs import Construct

class MidTermStack(Stack):
    def __init__(self, scope: Construct, id: str, identifier="", **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Create the source bucket (Bucket Src)
        self.bucket_src = s3.Bucket(
            self,
            "BucketSrc",
            bucket_name="bucket-srcmidterm-zy",
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # Create the destination bucket (Bucket Dst)
        self.bucket_dst = s3.Bucket(
            self,
            "BucketDst",
            bucket_name="bucket-dstmidterm-zy",
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # Create the DynamoDB table (Table T)
        self.table = dynamodb.Table(
            self,
            "TableT",
            table_name="table-tmidterm-zy",
            partition_key=dynamodb.Attribute(
                name="BucketName",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="CopyCreationTimestamp",
                type=dynamodb.AttributeType.NUMBER
            ),
            removal_policy=RemovalPolicy.DESTROY
        )

        # Add the Global Secondary Index (GSI1)
        self.table.add_global_secondary_index(
            index_name="DisownedIndex",
            partition_key=dynamodb.Attribute(
                name="IsDisownedFlag",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="DisownedAt",
                type=dynamodb.AttributeType.NUMBER  # Use NUMBER for timestamps
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )

    def get_resource_names(self):
        """
        Retrieves the resource names for integration or reference purposes.
        """
        return {
            "SourceBucketName": self.bucket_src.bucket_name,
            "DestinationBucketName": self.bucket_dst.bucket_name,
            "TableName": self.table.table_name,
        }
