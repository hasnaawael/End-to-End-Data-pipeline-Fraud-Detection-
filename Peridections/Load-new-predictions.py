
import os
# Set the environment variable for Python
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, BooleanType, LongType, TimestampType, DateType
from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.sql.functions import col, split, regexp_replace, udf
import math
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("test") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .config("spark.driver.memory", "8g") \
    .config("spark.yarn.appMasterEnv.PYSPARK_PYTHON", "/usr/bin/python3") \
    .getOrCreate()


# Load data from S3 bucket
sample_path = "s3://datalake-bucket-1212/New_Data/*"
df = spark.read.format("json").load(sample_path)

# Rename columns and cast to appropriate types
df = df.withColumnRenamed("Transaction Date/Time", "Transaction Date Time")

df = df.withColumn("Account Balance", df["Account Balance"].cast(DoubleType())) \
    .withColumn("Authorization Code", df["Authorization Code"].cast(IntegerType())) \
    .withColumn("Birth Date", df["Birth Date"].cast(DateType()))  \
    .withColumn("Card CVV", df["Card CVV"].cast(IntegerType())) \
    .withColumn("Card Number", df["Card Number"].cast(LongType())) \
    .withColumn("Cardholder's Age", df["Cardholder's Age"].cast(IntegerType())) \
    .withColumn("Customer Zip Code", df["Customer Zip Code"].cast(IntegerType())) \
    .withColumn("Latitude", df["Latitude"].cast(DoubleType())) \
    .withColumn("Longitude", df["Longitude"].cast(DoubleType())) \
    .withColumn("Loyalty Points Earned", df["Loyalty Points Earned"].cast(IntegerType())) \
    .withColumn("Merchant Category Code (MCC)", df["Merchant Category Code (MCC)"].cast(IntegerType())) \
    .withColumn("Merchant Rating", df["Merchant Rating"].cast(DoubleType())) \
    .withColumn("Recurring Transaction Flag", df["Recurring Transaction Flag"].cast(BooleanType())) \
    .withColumn("Remaining Balance", df["Remaining Balance"].cast(DoubleType())) \
    .withColumn("Response Time", df["Response Time"].cast(DoubleType())) \
    .withColumn("Transaction Amount", df["Transaction Amount"].cast(DoubleType())) \
    .withColumn("Transaction Date Time", df["Transaction Date Time"].cast(TimestampType())) \
    .withColumn("Transaction Risk Score", df["Transaction Risk Score"].cast(DoubleType())) \
    .withColumn("Fraud Flag", df["Fraud Flag"].cast(BooleanType())) \
    .withColumn("Location_ID_SK", col("Location_ID_SK").cast(IntegerType())) \
    .withColumn("Merchant ID", col("Merchant ID").cast(IntegerType()))

# Extract device information
df = df.withColumn('device_type', F.regexp_extract('Device Information', r"'device_type': '([^']+)'", 1)) \
       .withColumn('browser', F.regexp_extract('Device Information', r"'os': '\(?([a-zA-Z]+)", 1)) \
       .withColumn('os', F.regexp_extract('Device Information', r"'browser': '([^/]+)'", 1))

# Drop unnecessary columns
df = df.drop('Device Information', 'Email', 'Installment Information', "Cardholder's Income Bracket")

# Fill missing latitude and longitude with the average values
avg_latitude = df.select(F.mean(F.col("Latitude"))).collect()[0][0]
avg_longitude = df.select(F.mean(F.col("Longitude"))).collect()[0][0]
df = df.fillna({'Latitude': avg_latitude, 'Longitude': avg_longitude})

# Extract year, month, day, hour, and minute from the transaction date
df = df.withColumn('Year', F.year(F.col('Transaction Date Time'))) \
    .withColumn('Month', F.month(F.col('Transaction Date Time'))) \
    .withColumn('Day', F.dayofmonth(F.col('Transaction Date Time'))) \
    .withColumn('Hour', F.hour(F.col('Transaction Date Time'))) \
    .withColumn('Minute', F.minute(F.col('Transaction Date Time')))

# Split string columns
array_columns = ['Income Bracket', 'Transaction Status']
for column in array_columns:
    df = df.withColumn(column, split(col(column), "'")[1])

# Convert boolean flags to integer
df = df.withColumn('Fraud Flag', F.when(F.col('Fraud Flag') == True, 1).otherwise(0))
df = df.withColumn('Recurring Transaction Flag', F.when(F.col('Recurring Transaction Flag') == True, 1).otherwise(0))
####################################################################################################################################3

def haversine(lat1, lon1, lat2=0.0, lon2=0.0):
    R = 6371.0  # Radius of Earth in kilometers

    # Convert degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    
    # Differences in coordinates
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    # Haversine formula
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    # Distance in kilometers
    distance = R * c
    return distance

# UDF for PySpark
distance_udf = udf(lambda lat, lon: haversine(lat, lon), DoubleType())

# Assuming 'df' is your input DataFrame with 'Latitude' and 'Longitude' columns
df = df.withColumn("Distance", distance_udf(df["Latitude"], df["Longitude"]))


# List of categorical and numerical columns
categorical_cols = ['Transaction Type', 'Transaction Notes', 'Transaction Status', 
                    'Income Bracket', 'POS Entry Mode', 'browser']

numerical_cols = ['Transaction Amount', 'Transaction Risk Score', 'Account Balance', 
                  'Recurring Transaction Flag', 'Hour', 'Day', 'Minute', 
                  "Cardholder's Age", 'Remaining Balance', 'Merchant Rating', 
                  'Distance']

# String Indexing for categorical columns
indexers = [StringIndexer(inputCol=col, outputCol=col+"_index", handleInvalid="keep") 
            for col in categorical_cols]


# Vector Assembling for numerical columns + label-encoded categorical columns
feature_cols = [col+"_index" for col in categorical_cols] + numerical_cols
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# Standard Scaler for scaling features
scaler = StandardScaler(inputCol='features', outputCol='scaled_features', withMean=True, withStd=True)


# Define the pipeline stages and apply them
pipeline = Pipeline(stages=indexers + [assembler, scaler])
df_transformed = pipeline.fit(df).transform(df)

# Convert PySpark DataFrame to Pandas DataFrame
pandas_df = df_transformed.select('scaled_features', 'Fraud Flag','Transaction ID').toPandas()



##########################Loadmodel#############################
import joblib
from smart_open import open
from sklearn.ensemble import RandomForestClassifier
# S3 URL for the joblib Pickle file
# S3 URL for the joblib Pickle file
s3_url = 's3://datalake-bucket-1212/ML-Saved-Model/fraud_detection_model.pkl'

# Load the joblib Pickle model directly from S3
with open(s3_url, 'rb') as f:
    # Use joblib.load with the file-like object
    loaded_model = joblib.load(f)



# Extract features and target variable
X = pandas_df['scaled_features'].apply(lambda x: x.toArray()).tolist()
y = pandas_df['Fraud Flag'].tolist()

# Make predictions
predictions = loaded_model.predict(X)


# Step 1: Convert predictions to a list of Python native boolean types
predictions_python = [bool(pred) for pred in predictions]

# Step 2: Create a DataFrame from predictions
predictions_df = spark.createDataFrame(list(zip(pandas_df['Transaction ID'], predictions_python)), 
                                        schema=['Transaction ID', 'Predicted Fraud Flag'])

# Step 3: Join predictions with the original DataFrame
df_with_predictions = df.join(predictions_df, on='Transaction ID', how='left')







df_with_predictions = df_with_predictions.drop('Fraud Flag')

Final_df = df_with_predictions.withColumnRenamed("Predicted Fraud Flag", "Fraud Flag")


# Customer Dimension
customer_dimension = Final_df.select(
    "Customer ID", "First Name", "Last Name", "Country", "Birth Date", 
    "Phone Number", "Income Bracket", "Cardholder's Gender", 
    "Bank Name", "Card Issuer Country", "Card Type", 
    "Card Expiration Date", "Cardholder's Age", "Location_ID_SK"
).distinct()

# Location Dimension
location_dimension = Final_df.select(
    "Location_ID_SK", "Address", "City", "Customer State", 
    "Customer Zip Code", "Latitude", "Longitude", "Location"
).distinct()

# Date Dimension
date_dimension = Final_df.select(
    "Date_ID",
    F.to_timestamp("Transaction Date Time").alias("Full_date"),
    "Year", "Month", F.dayofweek("Transaction Date Time").alias("weekday"),
    "Day", "Hour", "Minute"
).distinct()

# Fraud Dimension
fraud_dimension = Final_df.select(
    "Fraud Flag", "Transaction Risk Score", "Transaction Notes", 
    "Fraud Detection Method", "Fraud_ID_SK"
).distinct()

# Merchant Dimension
merchant_dimension = Final_df.select("Merchant Name","Merchant ID").distinct()


# Transaction Fact Table
transaction_fact = Final_df.select(
    "Transaction ID", "Customer ID", "Merchant ID", "Transaction Amount", 
    "Transaction Type", "Transaction Status", "Transaction Channel", 
    "Response Time", "POS Entry Mode", "Recurring Transaction Flag", 
    "Loyalty Points Earned", "Currency", "Account Balance", 
    "Remaining Balance", "IP Address","Date_ID", "Fraud_ID_SK"
)
############################################################

output_base_path = "s3://datalake-bucket-1212/New_Data_Cleaning_Statging/" 

# Save Customer Dimension
customer_dimension.coalesce(1) \
    .write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save(f"{output_base_path}customer_dimension")



# Save Location Dimension
location_dimension.coalesce(1) \
    .write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save(f"{output_base_path}location_dimension")



# Save Date Dimension
date_dimension.coalesce(1) \
    .write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save(f"{output_base_path}date_dimension")


# Save Fraud Dimension
fraud_dimension.coalesce(1) \
    .write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save(f"{output_base_path}fraud_dimension")

# Save Merchant Dimension
merchant_dimension.coalesce(1) \
    .write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save(f"{output_base_path}merchant_dimension")


# Save Transaction Fact Table
transaction_fact.coalesce(1) \
    .write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save(f"{output_base_path}transaction_fact")

