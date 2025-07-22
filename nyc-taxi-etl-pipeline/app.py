from aws_cdk import App
from infrastructure.s3_stack import S3Stack
from infrastructure.glue_stack import GlueStack

app = App()

# Create S3 Bucket
s3_stack = S3Stack(app, "S3Stack")

# Create Glue resources with built-in EventBridge scheduling and Lambda trigger
glue_stack = GlueStack(app, "GlueStack",
    data_bucket=s3_stack.data_bucket.bucket_name,
    scripts_bucket=s3_stack.scripts_bucket.bucket_name,
    year="2024",
    month="01"
)

app.synth() 