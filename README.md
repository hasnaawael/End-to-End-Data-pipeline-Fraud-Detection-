🕵️‍♂️ Real Time Fraud Detection

🌟 Project Overview
This project implements a real-time fraud detection system leveraging AWS services. It processes streaming data to identify potential fraudulent activities and provides insightful visualizations.
🏗️ Architecture
Our system architecture integrates the following AWS components:
ComponentServiceDescription📊 Data Source-Origin of transaction data🖥️ IngestionAmazon EC2Hosts the Kafka producer🚰 StreamingApache Kafka & Amazon MSKManages real-time data streams🔥 Data CaptureAmazon Kinesis Data FirehoseCaptures and loads streaming data🗄️ StorageAmazon S3Data lake for raw and processed data⚡ ProcessingAWS EMR with Apache SparkData transformation and ML execution🏪 Data WarehouseAmazon RedshiftStores processed and aggregated data📢 NotificationsAmazon SNSSends alerts and notifications🔄 OrchestrationAWS Step FunctionsManages the data pipeline📊 VisualizationAmazon QuickSightCreates dashboards and insights
🔁 Data Flow

EC2-hosted Kafka producer ingests transaction data
Amazon MSK manages data streams
Kinesis Data Firehose loads data into S3
EMR cluster processes data using Spark
Processed data and predictions stored in Redshift
QuickSight generates visualizations from Redshift data
SNS sends fraud detection alerts
AWS Step Functions orchestrates the entire pipeline

🚀 Setup Instructions


![Data Pipeline Architecture](./Data%20pipeline%20Arch.png)
