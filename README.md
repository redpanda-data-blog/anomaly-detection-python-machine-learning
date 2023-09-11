# Anomaly Detection with Redpanda, Python, and Machine Learning

## Overview
This repository contains code for a simple anomaly detection pipeline using Kafka for data streaming and the Isolation Forest algorithm for anomaly detection. The code produces synthetic data with occasional anomalies, feeds it into a Kafka topic, consumes the data from that topic, performs anomaly detection, and then sends the detected anomalies into another Kafka topic.

## Prerequisites

- Python 3.x
- Kafka
- Scikit-learn
- NumPy

## Setup

1. Install Kafka on your machine and start the Kafka and ZooKeeper servers. [Follow this guide for installation](https://kafka.apache.org/quickstart).
2. Install the required Python packages:

`pip install kafka-python numpy scikit-learn`

## How to Run

1. Start the Kafka and ZooKeeper servers if they aren't already running.
2. Run the Python code:

`python your_python_script.py`

This will start producing synthetic data, consume it, detect anomalies, and then produce messages containing the detected anomalies.

## Code Structure

- `create_kafka_producer()`: Initialize a Kafka producer.
- `create_kafka_consumer()`: Initialize a Kafka consumer.
- `collect_raw_data()`: Collect raw data from a Kafka topic.
- `detect_anomalies()`: Detect anomalies using the Isolation Forest algorithm.
- `produce_anomalies()`: Send the detected anomalies to a Kafka topic.
