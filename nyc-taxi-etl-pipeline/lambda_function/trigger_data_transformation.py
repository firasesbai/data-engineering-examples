import boto3
import os

# Create a Glue client
glue_client = boto3.client('glue')

def lambda_handler(event, context):
    glue_job_name = os.environ['JOB_NAME']  
    data_bucket = os.environ['DATA_BUCKET']  
    # The year and month have default values that correspond to the example test data. 
    year = event.get('year', '2024')  
    month = event.get('month', '01')  

    try:
        response = glue_client.start_job_run(
            JobName=glue_job_name,
            Arguments={
                '--DATA_BUCKET': data_bucket,
                '--YEAR': year,
                '--MONTH': month
            }
        )
        
        return {
            'statusCode': 200,
            'body': f"Glue Job {glue_job_name} started successfully with Job Run ID: {response['JobRunId']}"
        }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f"Error starting Glue job: {str(e)}"
        }
