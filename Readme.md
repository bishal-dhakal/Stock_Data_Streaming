1. Real-Time Data Processing Application

This repository outlines a real-time data processing system designed to handle live stock updates and stream them to clients via WebSockets. Below is an overview of the architecture and the workflow.

2. Architecture Overview

2.1 Producer
Generates live stock data (e.g., price updates for 5 stocks).
Publishes the data to a Kafka topic for processing.

2.2 Consumer 1 (Database Writer)
Subscribes to the Kafka topic to consume the stock data.
Saves the raw stock data into a database for long-term storage and historical analysis.

2.3 Consumer 2 (Data Processor)
Subscribes to the same Kafka topic to consume the stock data.
Processes the data (e.g., formatting, enrichment, or filtering).
Saves the processed data to Redis for low-latency access.

2.4 FastAPI Application
Reads processed stock data from Redis.
Uses WebSockets to push real-time updates to connected clients.