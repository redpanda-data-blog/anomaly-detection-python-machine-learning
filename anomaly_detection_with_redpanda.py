#Import all required libraries
from kafka.producer import KafkaProducer
import numpy as np
import json

raw_data_producer = KafkaProducer(bootstrap_servers='localhost:9092')

#Produce synthetic data and the occasional anomalies
for i in range(1000):
    value = np.sin(i * 0.1)
    
    # Introduce a random spike as an anomaly with 1% probability
    if np.random.rand() < 0.01:
        value += np.random.rand() * 5

    # Create a dictionary with both value and time-step
    data = {
        "value": value,
        "time_step": i
    }

    # Convert the dictionary to a JSON string and encode it
    raw_data_producer.send('raw-data', value=json.dumps(data).encode("utf-8"))

raw_data_producer.flush()
raw_data_producer.close()



#Import all required libraries
from kafka.consumer import KafkaConsumer

# Create a consumer to read raw data
raw_data_consumer = KafkaConsumer('raw-data', bootstrap_servers='localhost:9092')

# Collect data for training and prediction, along with time-steps
data = []
time_steps = []
for i, msg in enumerate(raw_data_consumer):
    value = float(msg.value.decode('utf-8'))
    data.append([value])
    time_steps.append(i)  # Assuming each data point corresponds to a time-step

    # Stop collecting after 1000 data points
    if len(data) >= 1000:
        break

#Import all required libraries
from sklearn.ensemble import IsolationForest


# Convert to a NumPy array for Scikit-learn
X = np.array(data)

# Train an Isolation Forest model
model = IsolationForest(contamination=0.01)
model.fit(X)

# Predict anomalies
pred = model.predict(X)

# Anomalies are represented by -1
anomalies = X[pred == -1]
anomaly_time_steps = [time_steps[i] for i in range(len(pred)) if pred[i] == -1]


# Create a producer to send anomalies
anomalies_producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Produce the detected anomalies and time-steps to the Redpanda topic
for anomaly, time_step in zip(anomalies, anomaly_time_steps):
    anomaly_data = {
        "anomalous_value": anomaly[0],
        "time_step": time_step
    }
    anomalies_producer.send('anomalies', value=json.dumps(anomaly_data))

anomalies_producer.flush()

# Print the results along with corresponding time-steps
print("Detected anomalies:")
for anomaly, time_step in zip(anomalies, anomaly_time_steps):
    print(f"Value: {anomaly[0]}, Time-step: {time_step}")


