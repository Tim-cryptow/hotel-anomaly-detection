import boto3
import sagemaker
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sagemaker.amazon.amazon_estimator import get_image_uri
from sagemaker.session import Session
from sagemaker.inputs import TrainingInput
from sagemaker.serializers import CSVSerializer
from sagemaker.deserializers import JSONDeserializer

# Initializing SageMaker session and getting role
sagemaker_session = sagemaker.Session()
role = sagemaker.get_execution_role()

# Defining the data path
bucket = 'hotel-anomaly-detection-data-goodluck'
csv_file = 'hotel_data.csv'
training_data_path = f's3://{bucket}/{csv_file}'
output_path = f's3://{bucket}/model-output/'

def preprocess_data(df):
    print("Original DataFrame shape:", df.shape)
# Note: The current timestamp format keeps throwing an error in sagemaker. Since the data was generated, this won't cause a problem. To address this, the timestamp column was regenrated.
    df = df.drop('timestamp', axis=1)

    # Generating random dates within 2024
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)
    random_dates = [
        start_date + timedelta(
            seconds=np.random.randint(0, int((end_date - start_date).total_seconds()))
        )
        for _ in range(len(df))
    ]

    # Creating a new 'timestamp' column with random dates
    df['timestamp'] = random_dates

    # Extracting hour from the timestamp
    df['hour'] = df['timestamp'].dt.hour.astype('float64')

    # Selecting ony numerical features for anomaly detection
    features = ['occupancy_rate', 'bookings', 'cancellations', 'revenue_per_room', 'average_stay_length', 'hour']
    for feature in features:
        df[feature] = pd.to_numeric(df[feature], errors='coerce')

    # Note: Despite the fact that there were six 6 columns, the randomcut keep requesting for a 7 feature_dim value even though the feature_dim was explitly set to 6.
    # To address this, a dummy feature was added to make it 7 fields as expected by the algorithm
    df['dummy'] = 0

    df_cleaned = df[features + ['dummy']].fillna(df[features].mean())

    print("\nCleaned DataFrame:")
    print(df_cleaned.head())
    return df_cleaned

s3 = boto3.client('s3')
obj = s3.get_object(Bucket=bucket, Key=csv_file)
df = pd.read_csv(obj['Body'])
preprocessed_data = preprocess_data(df)
print(preprocessed_data.head())


# Saving preprocessed data as CSV
preprocessed_file = 'preprocessed_data.csv'
preprocessed_data.to_csv(f'/tmp/{preprocessed_file}', index=False, header=False)
s3.upload_file(f'/tmp/{preprocessed_file}', bucket, preprocessed_file)

# Setting up the RandomCutForest estimator
container = get_image_uri(sagemaker_session.boto_region_name, 'randomcutforest')

rcf = sagemaker.estimator.Estimator(
    container,
    role,
    instance_count=1,
    instance_type='ml.m5.large',
    output_path=output_path,
    sagemaker_session=sagemaker_session
)


rcf.set_hyperparameters(
    num_samples_per_tree=512,
    num_trees=50,
    feature_dim=6
)

# Creating TrainingInput object
train_input = TrainingInput(
    s3_data=f's3://{bucket}/{preprocessed_file}',
    content_type='text/csv',
    s3_data_type='S3Prefix',
    distribution='ShardedByS3Key'
)

# Training the model
rcf.fit({'train': train_input})

# Deploying the model
rcf_predictor = rcf.deploy(
    initial_instance_count=1,
    instance_type='ml.m5.large',
    serializer=CSVSerializer(),
    deserializer=JSONDeserializer()
)

print(f"Model endpoint: {rcf_predictor.endpoint_name}")