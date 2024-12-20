# hotel-anomaly-detection
This project implements a real-time anomaly detection system for hotel data using AWS services.

## Features
- Data generation and ingestion using Kinesis
- Data storage in S3
- Anomaly detection using SageMaker's RandomCutForest algorithm
- Real-time processing with AWS Lambda

## Setup
1. AWS Account Setup:
   - Create an AWS account if you don't have one
   - Set up IAM user with appropriate permissions for S3, Kinesis, Lambda, and SageMaker

2. Environment Setup:
   - Install Python 3.8 or later
   - Install AWS CLI and configure with your credentials. Alternatively, use AWS Management Console 

3. Clone the repository:

## Architecture
The system architecture consists of the following components:

1. Data Generation: A Python script (`data_generator.py`) simulates hotel data and sends it to an AWS Kinesis Data Stream.

2. Data Storage: AWS Lambda function (`Kinesis-to-S3-lambda.py`) is triggered by the Kinesis stream, processes the data, and stores it in an S3 bucket.

3. Anomaly Detection:
- A SageMaker RandomCutForest model is trained on historical data using `model-training-and-deployment.py`.
- The Lambda function sends each new data point to the deployed model for real-time anomaly detection.

4. Result Storage: Detected anomalies are stored in a DynamoDB table.


Data Generator -> Kinesis Stream -> Lambda -> S3 (storage) & SageMaker (detection)

This architecture can allow for real-time processing and scalability, leveraging AWS managed services for robust and efficient anomaly detection.

## Technologies Used
- AWS (S3, Kinesis, Lambda, SageMaker)
- Python

## License
- None
