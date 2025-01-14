import boto3
import botocore.exceptions

# Initialize AWS Glue client
glue_client = boto3.client('glue', region_name='us-east-1') 

# Text to search in Glue job scripts
SEARCH_TEXT = "metrics_assortment"  


def get_all_glue_jobs():
    """Retrieve all AWS Glue job names with error handling."""
    job_names = []
    try:
        paginator = glue_client.get_paginator('get_jobs')
        for page in paginator.paginate():
            for job in page['Jobs']:
                job_names.append(job['Name'])
    except botocore.exceptions.NoCredentialsError:
        print("Error: AWS credentials not found.")
    except botocore.exceptions.PartialCredentialsError:
        print("Error: Incomplete AWS credentials.")
    except botocore.exceptions.ClientError as e:
        print(f"ClientError: {e.response['Error']['Message']}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    return job_names


def search_text_in_glue_job(job_name, search_text):
    """search for text in glub fetched glue jobs."""
    try:
        response = glue_client.get_job(JobName=job_name)
        script_location = response['Job']['Command'].get('ScriptLocation')
        
        if not script_location:
            print(f"No script location found for job: {job_name}")
            return False

        # Extract script from S3
        s3_client = boto3.client('s3')
        s3_bucket, s3_key = script_location.replace("s3://", "").split("/", 1)
        script_object = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        script_content = script_object['Body'].read().decode('utf-8')

        if search_text in script_content:
            print(f"✅ Text found in job: {job_name}")
            return True

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchBucket':
            print(f"Error: S3 bucket does not exist for job {job_name}")
        elif e.response['Error']['Code'] == 'NoSuchKey':
            print(f"Error: Script file does not exist in S3 for job {job_name}")
        else:
            print(f"ClientError in job {job_name}: {e.response['Error']['Message']}")
    except UnicodeDecodeError:
        print(f"Error: Failed to decode the script content for job {job_name}")
    except Exception as e:
        print(f"Unexpected error in job {job_name}: {e}")

    return False


def main():
    """Main function to iterate through jobs and search text."""
    try:
        jobs = get_all_glue_jobs()
        if not jobs:
            print("No Glue jobs found or failed to fetch jobs.")
            return

        print(f"Found {len(jobs)} Glue jobs.")
        
        count = 1

        matched_jobs = []
        for job in jobs:
            if search_text_in_glue_job(job, SEARCH_TEXT):
                matched_jobs.append(job)

            print("Jobs read:", count)
            count+=1
        
        print("\n🎯 Jobs containing the search text:")
        if matched_jobs:
            for job in matched_jobs:
                print(f"- {job}")
        else:
            print("No jobs contained the specified text.")

    except KeyboardInterrupt:
        print("\nOperation interrupted by the user.")
    except Exception as e:
        print(f"Unexpected error in main function: {e}")


if __name__ == "__main__":
    main()
