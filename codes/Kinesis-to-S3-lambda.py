import boto3
import json
import os
import base64
import csv
import io
from datetime import datetime

s3 = boto3.client('s3')

def decode_kinesis_data(encoded_data):
    """Decode base64 encoded Kinesis data"""
    try:
        decoded_data = base64.b64decode(encoded_data).decode('utf-8')
        print(f"Decoded data: {decoded_data}")
        return json.loads(decoded_data)
    except Exception as e:
        print(f"Error decoding data: {e}")
        raise

def process_records(records):
    """Process and format records for storage"""
    processed_data = []
    print(f"Processing {len(records)} records")
    
    for record in records:
        try:
            # Decode and parse the record data
            kinesis_data = record['kinesis']['data']
            print(f"Processing record with data: {kinesis_data}")
            
            payload = decode_kinesis_data(kinesis_data)
            processed_data.append(payload)
            print(f"Successfully processed record: {payload}")
        except Exception as e:
            print(f"Error processing record: {e}")
            continue
    
    print(f"Processed {len(processed_data)} records successfully")
    return processed_data

def fetch_existing_data(bucket_name, file_key):
    """Fetch existing data from S3 if the file exists"""
    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        body = response['Body'].read().decode('utf-8')
        csv_reader = csv.reader(io.StringIO(body))
        existing_data = list(csv_reader)
        print(f"Existing data fetched with {len(existing_data)} rows")
        return existing_data
    except s3.exceptions.NoSuchKey:
        print(f"No existing file found: {file_key}")
        return []
    except Exception as e:
        print(f"Error fetching existing file: {e}")
        raise

def save_to_s3(data, timestamp):
    """Save processed data to S3"""
    if not data:
        print("No data to save to S3")
        return

    try:
        bucket_name = os.environ.get('S3_BUCKET_NAME')
        if not bucket_name:
            raise ValueError("S3_BUCKET_NAME environment variable is not set")

        file_name = "hotel_data.csv"
        full_path = f"hotel_data/{file_name}"

        csv_content = ""

        # Checking if the file already exists in the S3 bucket
        try:
            print(f"Checking if {full_path} exists in bucket: {bucket_name}")
            s3.head_object(Bucket=bucket_name, Key=full_path)

            # If the file exists, download and exclude headers
            existing_object = s3.get_object(Bucket=bucket_name, Key=full_path)
            existing_content = existing_object['Body'].read().decode('utf-8')

            print("File exists. Appending to the existing content.")
            csv_content += existing_content
        except s3.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("File does not exist. Creating a new file.")
                # Adding headers for a new file
                csv_content += "hotel_id,timestamp,occupancy_rate,bookings,cancellations,revenue_per_room,average_stay_length\n"
            else:
                raise

        # Appending new records to the CSV content
        for record in data:
            csv_content += f"{record['hotel_id']},{record['timestamp']},{record['occupancy_rate']},{record['bookings']},{record['cancellations']},{record['revenue_per_room']},{record['average_stay_length']}\n"

        # Uploading the updated content back to S3
        response = s3.put_object(
            Bucket=bucket_name,
            Key=full_path,
            Body=csv_content
        )

        print(f"S3 put_object response: {response}")
        print(f"Successfully updated {file_name} in S3 bucket: {bucket_name}")

    except Exception as e:
        print(f"Error saving to S3: {e}")
        raise

def lambda_handler(event, context):
    try:
        print("Starting to process records...")
        print(f"Event: {json.dumps(event)}")
        
        processed_records = process_records(event['Records'])
        
        # Generating timestamp for the batch
        timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        
        # Saving to S3
        save_to_s3(processed_records, timestamp)
        
        return {
            'statusCode': 200,
            'body': json.dumps('Data processed and saved successfully')
        }
    except Exception as e:
        print(f"Error in lambda_handler: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error processing data: {str(e)}')
        }
