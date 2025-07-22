from aws_cdk import (
    Stack,
    aws_glue as glue,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    aws_events as events,
    aws_events_targets as targets,
    aws_lambda as _lambda, 
    CfnParameter,
    CfnOutput
)
from constructs import Construct

class GlueStack(Stack):
    def __init__(self, scope: Construct, id: str, data_bucket: str, year: str, month: str,
                 scripts_bucket: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        glue_job_name_param = CfnParameter(self, "GlueJobName",
            type="String",
            description="NYC Taxi Glue Job name",
            default="nyc-taxi-transformation-job"
        )

        glue_script_path_param = CfnParameter(self, "GlueScriptPath",
            type="String",
            description="S3 path for NYC Taxi Glue script (relative to bucket)",
            default="scripts/data_transformation_job.py"
        )

        glue_temp_dir_param = CfnParameter(self, "GlueTempDir",
            type="String",
            description="S3 path for Glue temporary directory (relative to bucket)",
            default="temp/"
        )

        # IAM Role for Glue
        glue_role = iam.Role(self, "GlueServiceRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess")
            ]
        )

        # Upload the Glue script to S3
        s3deploy.BucketDeployment(self, "DeployGlueScript",
            sources=[s3deploy.Source.asset("glue_script")], 
            destination_bucket=s3.Bucket.from_bucket_name(self, "ScriptsBucket", scripts_bucket),
            destination_key_prefix="scripts" 
        )

        # Glue Job Definition
        glue_job = glue.CfnJob(self, "GlueJob",
            name=glue_job_name_param.value_as_string,
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{scripts_bucket}/{glue_script_path_param.value_as_string}",
                python_version="3"
            ),
            default_arguments={
                "--TempDir": f"s3://{scripts_bucket}/{glue_temp_dir_param.value_as_string}",
                "--job-language": "python",
                "--enable-glue-datacatalog": "true",
                "--DATA_BUCKET": data_bucket
            },
            glue_version="5.0",
            number_of_workers=10,
            worker_type="G.1X"
        )

        # Glue Crawler Definition
        glue_crawler = glue.CfnCrawler(self, "GlueCrawler",
            role=glue_role.role_arn,
            database_name="nyc_taxi_database",
            name="nyc-taxi-crawler",
            table_prefix="nyc_taxi_",
            targets={
                "s3Targets": [{"path": f"s3://{data_bucket}/processed/"}]
            },
            schedule={
                "schedule_expression": "cron(0 5 * * *)"  # 5 AM UTC daily
            },
            schema_change_policy={
                "updateBehavior": "UPDATE_IN_DATABASE",
                "deleteBehavior": "DEPRECATE_IN_DATABASE"
            }
        )

        # Lambda function to trigger the Glue job
        glue_trigger_lambda = _lambda.Function(
            self, "GlueTriggerLambda",
            runtime=_lambda.Runtime.PYTHON_3_9, 
            handler="trigger_data_transformation.lambda_handler", 
            code=_lambda.Code.from_asset("lambda_function"),
            environment={
                "JOB_NAME": glue_job_name_param.value_as_string,
                "DATA_BUCKET": data_bucket,
                "YEAR": year,
                "MONTH": month
            },
        )

        # Grant the Lambda function permissions to start Glue jobs
        glue_trigger_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["glue:StartJobRun"],
                resources=[f"arn:aws:glue:{self.region}:{self.account}:job/{glue_job.name}"] 
            )
        )

        # EventBridge Rule to trigger Glue job daily at 4 AM UTC via Lambda
        event_rule = events.Rule(
            self, "DailyNYCTaxiETLRule",
            schedule=events.Schedule.cron(minute="0", hour="4"),  # 4:00 AM UTC
            description="Trigger NYC Taxi ETL job daily via Lambda"
        )

        # Add Lambda function as target for EventBridge rule
        event_rule.add_target(
            targets.LambdaFunction(glue_trigger_lambda)
        )
        
        # Expose the Glue job name for other stacks to reference
        self.glue_job_name = glue_job.name
        
        CfnOutput(self, "GlueJobOutput",
            value=glue_job.name,
            description="NYC Taxi Glue job name"
        )

        CfnOutput(self, "GlueCrawlerOutput",
            value=glue_crawler.name,
            description="NYC Taxi Glue crawler name"
        )

        CfnOutput(self, "GlueRoleOutput",
            value=glue_role.role_arn,
            description="NYC Taxi Glue IAM Role ARN"
        ) 