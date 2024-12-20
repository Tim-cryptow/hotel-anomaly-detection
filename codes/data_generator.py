import boto3
import json
import random
import time
from datetime import datetime

kinesis = boto3.client('kinesis', region_name='us-east-1')

def generate_hotel_data():
    # Generating data for 10 hotels
    hotel_id = random.randint(1, 10)
    
    # Generating realistic occupancy data
    base_occupancy = 0.7  # 70% average occupancy
    time_variation = random.uniform(-0.2, 0.2)  
    seasonal_factor = random.uniform(0.9, 1.1)  
    
    occupancy_rate = min(1.0, max(0.0, (base_occupancy + time_variation) * seasonal_factor))
    
    # Generating correlated bookings based on occupancy
    max_rooms = 100
    bookings = int(occupancy_rate * max_rooms * random.uniform(0.8, 1.2))
    
    # Generating realistic cancellation rate (3-8% of bookings)
    cancellations = int(bookings * random.uniform(0.03, 0.08))
    
    return {
        'hotel_id': hotel_id,
        'timestamp': datetime.now().isoformat(),
        'occupancy_rate': round(occupancy_rate, 3),
        'bookings': bookings,
        'cancellations': cancellations,
        'revenue_per_room': round(random.uniform(80, 200), 2),
        'average_stay_length': round(random.uniform(1.5, 4.5), 1)
    }

def send_to_kinesis(data):
    try:
        response = kinesis.put_record(
            StreamName='HotelDataStream',
            Data=json.dumps(data),
            PartitionKey=str(data['hotel_id'])
        )
        print(f"Data sent successfully: {data}")
        return response
    except Exception as e:
        print(f"Error sending data to Kinesis: {e}")
        return None

def main():
    print("Starting data generation...")
    while True:
        try:
            data = generate_hotel_data()
            send_to_kinesis(data)
            time.sleep(5)
        except KeyboardInterrupt:
            print("\nStopping data generation...")
            break
        except Exception as e:
            print(f"Unexpected error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()

