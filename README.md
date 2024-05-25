# Reddit Analysis Project

Welcome to the Reddit Analysis Project! This project is designed to perform real-time analysis on Reddit data using Apache Kafka, Apache Spark Streaming, and sentiment analysis. The pipeline involves several stages, including data ingestion, filtering, sentiment analysis, and batch processing.

## Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Overview
This project fetches data from Reddit, processes it through a Kafka producer, and sends it to a Kafka consumer. The data is then filtered using Spark Streaming, and two separate consumers handle further processing: one for sentiment analysis and another for storing the data in a database. Additionally, batch processing is performed at regular intervals to analyze the entire dataset.

## Architecture
The project architecture consists of the following components:

1. **Reddit Data Ingestion**:
   - A Kafka producer fetches data from Reddit and publishes it to a Kafka topic.

2. **Spark Streaming Filter**:
   - A Kafka consumer receives the data and filters out unnecessary information using Spark Streaming.
   - The filtered data is published back to another Kafka topic.

3. **Stream Sentiment Analysis**:
   - One Kafka consumer reads the filtered data and performs sentiment analysis using Spark Streaming.
   - The analyzed data is published to another Kafka topic or processed further as needed.

4. **Database Storage**:
   - Another Kafka consumer reads the filtered data and stores it in a database for further analysis and retrieval.

5. **Batch Processing**:
   - At regular intervals, batch processing is performed on the entire dataset to provide comprehensive analysis.

## Prerequisites
Before setting up the project, ensure you have the following installed:
- Docker
- Docker Compose
- Python 3.x
- Reddit API credentials (client ID and secret)

## Setup
### 1. Clone the Repository
```bash
git clone https://github.com/Ashenoy64/Reddit-Analysis.git
cd Reddit-Analysis
```

### 2. Install Python Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure Environment Variables
Set your Reddit API credentials and Kafka configuration in an `.env` file in the project root:

### 4. Start Kafka and Spark Cluster
Start the Kafka and Spark cluster using Docker Compose:
```bash
docker-compose up -d
```

### 5. Compile Docker Images
Navigate to the respective directories and build the Docker images for the producer and consumers if Dockerfiles are available:
- For Producer:
  ```bash
  cd producer
  docker build -t reddit-producer .
  ```
- For Filter Consumer (if applicable):
  ```bash
  cd consumer/FilterConsumer
  docker build -t filter-consumer .
  ```
- For Database Consumer (if applicable):
  ```bash
  cd consumer/database_consumer
  docker build -t database-consumer .
  ```
- For Post Filtering Consumer (if applicable):
  ```bash
  cd consumer/post_filtering_consumer
  docker build -t post-filtering-consumer .
  ```

If Dockerfiles are not available, you can run the Python scripts directly.

### 6. Setup Frontend
Navigate to the `frontend` directory, install requirements, and start the frontend application:
```bash
cd frontend
pip install -r requirements.txt
python main.py
```

### 7. Configure Streamlit UI
Access the Streamlit UI to configure streaming and batch processing settings. After submitting it should deploy 
set of container.


## Usage
1. **Access Streamlit UI**:
   Open your web browser and go to the Streamlit UI to configure and monitor the streaming and batch processing.

2. **Monitor Data Flow**:
   Use the UI and logs to monitor the flow of data through the various components.

3. **Perform Analysis**:
   Analyze the data as it flows through the system, and perform batch processing at regular intervals.

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
