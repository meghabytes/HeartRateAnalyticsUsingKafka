from kafka import KafkaProducer
import csv
import time

# Kafka configuration
bootstrap_servers = 'localhost:9092'

# Create Kafka producers for each topic
producer_topic1 = KafkaProducer(bootstrap_servers=bootstrap_servers)
producer_topic2 = KafkaProducer(bootstrap_servers=bootstrap_servers)
producer_topic3 = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Function to send message to Kafka for each topic
def send_message(producer, topic, message):
    producer.send(topic, message.encode('utf-8'))

# Read CSV file and send relevant content to each Kafka topic
def send_csv_to_kafka(file_path, topic1, topic2, topic3):
    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            if len(row) >= 3:  # Ensure row has at least 3 columns
                # Extract values from the row
                value1 = row[0]  # First value
                value2 = row[1]  # Second value
                value3 = row[2]  # Third value
                
                # Send each value to its respective Kafka topic
                send_message(producer_topic1, topic1, value1)
                send_message(producer_topic2, topic2, value2)
                send_message(producer_topic3, topic3, value3)
                
                time.sleep(0.5)  # Add a 1-second delay between sending messages

# Main function
if __name__ == "__main__":
    csv_file_path = "heart_rate.csv"
    kafka_topic1 = "topic1"
    kafka_topic2 = "topic2"
    kafka_topic3 = "topic3"
    send_csv_to_kafka(csv_file_path, kafka_topic1, kafka_topic2, kafka_topic3)
