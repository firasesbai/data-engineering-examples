from aws_cdk import (
    aws_s3 as s3,
    RemovalPolicy,
    CfnParameter,
    CfnOutput,
    Stack
)
from constructs import Construct

class S3Stack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Define parameters for bucket names
        data_bucket_param = CfnParameter(self, "DataBucketName",
            type="String",
            description="Name of the S3 bucket for NYC taxi data (landing and processed)"
        )

        scripts_bucket_param = CfnParameter(self, "ScriptsBucketName",
            type="String",
            description="Name of the S3 bucket to store Glue scripts for NYC taxi ETL"
        )

        # Create main data bucket (combines landing and processed)
        self.data_bucket = s3.Bucket(self, "DataBucket",
            bucket_name=data_bucket_param.value_as_string,
            versioned=True,
            removal_policy=RemovalPolicy.RETAIN
        )

        # Create scripts bucket
        self.scripts_bucket = s3.Bucket(self, "ScriptsBucket",
            bucket_name=scripts_bucket_param.value_as_string,
            versioned=True,
            removal_policy=RemovalPolicy.RETAIN
        )

        # Output the bucket names after deployment
        CfnOutput(self, "DataBucketNameOutput",
            value=self.data_bucket.bucket_name,
            description="NYC Taxi Data S3 Bucket (landing and processed)"
        )

        CfnOutput(self, "ScriptsBucketNameOutput",
            value=self.scripts_bucket.bucket_name,
            description="S3 Bucket for NYC Taxi ETL Glue Scripts"
        ) 