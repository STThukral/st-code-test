import boto3
import datetime

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Get the S3 bucket name and object key (file name) from the event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    old_key = event['Records'][0]['s3']['object']['key']
    
    # Extract the current date in YYYY-MM-DD format for the new file name
    current_date = datetime.datetime.now().strftime('%Y-%m-%d')
    
    # Construct the new file name, raw_file_<date>.csv
    new_key = f'raw_file_{current_date}.csv'
    
    try:
        # Copy the original file to the new name
        s3.copy_object(CopySource={'Bucket': bucket_name, 'Key': old_key},
                       Bucket=bucket_name, Key=new_key)
        
        # Delete the original file
        s3.delete_object(Bucket=bucket_name, Key=old_key)
        
        print(f"File {old_key} renamed to {new_key}")
        
        return {
            'statusCode': 200,
            'body': f"File {old_key} renamed to {new_key}"
        }
        
    except Exception as e:
        print(f"Error renaming file {old_key}: {str(e)}")
        
        return {
            'statusCode': 500,
            'body': f"Error renaming file {old_key}: {str(e)}"
        }
