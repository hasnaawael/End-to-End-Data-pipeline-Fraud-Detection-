🕵️‍♂️ Real Time Fraud Detection
🌟 Project Overview
This project implements a real-time fraud detection system using AWS services. It processes streaming data to detect potential fraudulent activities and provides visualization for insights.
🏗️ Architecture
The system architecture consists of the following components:

📊 Data Source: Origin of transaction data.
🖥️ Amazon EC2: Hosts the Kafka producer.
🚰 Apache Kafka: Used for ingesting real-time data streams.
🌊 Amazon Managed Streaming for Apache Kafka (MSK): Manages the Kafka cluster.
🔥 Amazon Kinesis Data Firehose: Captures and loads streaming data.
🗄️ Amazon S3: Data lake for storing raw and processed data.
⚡ AWS EMR (with Apache Spark): For data transformation, processing, and ML model execution.
🏪 Amazon Redshift: Data warehouse for storing processed and aggregated data.
📢 Amazon SNS: For sending notifications and alerts.
🔄 AWS Step Functions: Orchestrates the data pipeline.
📊 Amazon QuickSight: For data visualization.

🔁 Data Flow

Transaction data is ingested through a Kafka producer running on EC2.
Data streams are managed by Amazon MSK.
Kinesis Data Firehose captures the streaming data and loads it into S3.
EMR cluster processes the data using Spark, applying transformations and ML models.
Processed data and predictions are stored in Redshift.
QuickSight connects to Redshift for creating visualizations.
If fraud is detected, alerts are sent via SNS.
The entire pipeline is orchestrated using AWS Step Functions.


![Data Pipeline Architecture](./Data%20pipeline%20Arch.png)
