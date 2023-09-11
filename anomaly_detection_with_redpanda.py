from kafka.producer import KafkaProducer
from kafka.consumer import KafkaConsumer
import numpy as np
import json
from sklearn.ensemble import IsolationForest

def create_kafka_producer(bootstrap_servers):
    """
    Create and return a Kafka producer.
    """
    return KafkaProducer(bootstrap_servers=bootstrap_servers)

def produce_synthetic_data(producer, topic_name, num_data_points=1000):
    """
    Produce synthetic data with occasional anomalies and send it to a Kafka topic.
    """
    for i in range(num_data_points):
        value = np.sin(i * 0.1)
        
        if np.random.rand() < 0.01:
            value += np.random.rand() * 5

        data = {
            "value": value,
            "time_step": i
        }
        producer.send(topic_name, value=json.dumps(data).encode("utf-8"))

    producer.flush()

def create_kafka_consumer(topic_name, bootstrap_servers):
    """
    Create and return a Kafka consumer.
    """
    return KafkaConsumer(topic_name, bootstrap_servers=bootstrap_servers)

def collect_raw_data(consumer, max_data_points=1000):
    """
    Collect raw data from a Kafka topic.
    """
    data, time_steps = [], []
    
    for i, msg in enumerate(consumer):
        value = json.loads(msg.value.decode('utf-8'))['value']
        data.append([value])
        time_steps.append(i)

        if len(data) >= max_data_points:
            break
            
    return np.array(data), time_steps

def detect_anomalies(X, contamination=0.01):
    """
    Detect anomalies in the data using the Isolation Forest algorithm.
    """
    model = IsolationForest(contamination=contamination)
    model.fit(X)
    pred = model.predict(X)
    anomalies = X[pred == -1]
    anomaly_indices = [i for i, p in enumerate(pred) if p == -1]
    return anomalies, anomaly_indices

def produce_anomalies(producer, topic_name, anomalies, anomaly_time_steps):
    """
    Send detected anomalies to a Kafka topic.
    """
    for anomaly, time_step in zip(anomalies, anomaly_time_steps):
        anomaly_data = {
            "anomalous_value": anomaly[0],
            "time_step": time_step
        }
        producer.send(topic_name, value=json.dumps(anomaly_data))

    producer.flush()


if __name__ == "__main__":
    bootstrap_servers = 'localhost:9092'

    # Produce synthetic data
    raw_data_producer = create_kafka_producer(bootstrap_servers)
    produce_synthetic_data(raw_data_producer, 'raw-data')
    raw_data_producer.close()

    # Consume raw data
    raw_data_consumer = create_kafka_consumer('raw-data', bootstrap_servers)
    X, time_steps = collect_raw_data(raw_data_consumer)

    # Detect anomalies
    anomalies, anomaly_time_steps = detect_anomalies(X)

    # Produce anomalies
    anomalies_producer = create_kafka_producer(bootstrap_servers)
    produce_anomalies(anomalies_producer, 'anomalies', anomalies, anomaly_time_steps)
    anomalies_producer.close()

    # Print results
    print("Detected anomalies:")
    for anomaly, time_step in zip(anomalies, anomaly_time_steps):
        print(f"Value: {anomaly[0]}, Time-step: {time_step}")
