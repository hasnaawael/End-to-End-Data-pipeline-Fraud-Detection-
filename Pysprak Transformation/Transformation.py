from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("Data Transformation") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()


from pyspark.sql import functions as F
from pyspark.sql.types import *

sample_path = "s3://datalake-bucket-1212/*/*/*/*/*"
df = spark.read.format("json").load(sample_path)


df.cache()
from pyspark.sql.types import DoubleType, IntegerType, BooleanType, LongType, TimestampType
from pyspark.sql.functions import col


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
    .withColumn('os', F.regexp_extract('Device Information', r"'os': '([^/]+)", 1)) \
    .withColumn('browser', F.regexp_extract('Device Information', r"'browser': '\(?([a-zA-Z]+)", 1))



#drop unnecessary colume
df = df.drop('Device Information', 'Email', 'Installment Information', "Cardholder's Income Bracket")






# Calculate the average for Latitude and Longitude
avg_latitude = df.select(F.mean(F.col("Latitude"))).collect()[0][0]
avg_longitude = df.select(F.mean(F.col("Longitude"))).collect()[0][0]
df = df.fillna({'Latitude': avg_latitude, 'Longitude': avg_longitude})



# Transaction Date Time >> Y , M , D
df = df.withColumn('Year', F.year(F.col('Transaction Date Time'))) \
    .withColumn('Month', F.month(F.col('Transaction Date Time'))) \
    .withColumn('Day', F.dayofmonth(F.col('Transaction Date Time'))) \
    .withColumn('Hour', F.hour(F.col('Transaction Date Time'))) \
    .withColumn('Minute', F.minute(F.col('Transaction Date Time')))


from pyspark.sql.functions import col, expr, regexp_replace, to_date, to_timestamp, when, udf, split
array_columns = ['Income Bracket', 'Transaction Status']
for column in array_columns:
    df = df.withColumn(column, split(col(column), "'")[1]) 
    

#######################################################################################################################

# # Step 1: Group by 'Merchant Name' and create unique Merchant_ID
# merchant_counts = df.groupBy("Merchant Name").count()

# # Add a unique Merchant_ID for each merchant
# merchant_counts = merchant_counts.withColumn("Merchant_ID", F.monotonically_increasing_id())

# # Step 2: Join the merchant_counts DataFrame back to the original DataFrame
# df = df.join(merchant_counts.select("Merchant Name", "Merchant_ID"), on="Merchant Name", how="left")

# from pyspark.sql.functions import date_format, col
# df = df.withColumn("Date_ID", date_format(col("Transaction Date Time"), "yyyyMMddHHmm"))

# # Create a unique Location_Id for each customer
# # You can use the Customer ID column and other identifying columns to create the Location_Id
# df = df.withColumn("Location_ID_SK", F.abs(F.hash(F.concat_ws("_", "Customer ID", "Address", "City", "Country"))))

# # Create a unique Fraud_id for each transaction, possibly based on the Transaction ID
# df = df.withColumn("Fraud_ID_SK", F.monotonically_increasing_id())

#################################################################################################

# Customer Dimension
customer_dimension = df.select(
    "Customer ID", "First Name", "Last Name", "Country", "Birth Date", 
    "Phone Number", "Income Bracket", "Cardholder's Gender", 
    "Bank Name", "Card Issuer Country", "Card Type", 
    "Card Expiration Date", "Cardholder's Age", "Location_ID_SK"
).distinct()


# Location Dimension
location_dimension = df.select(
    "Location_ID_SK", "Address", "City", "Customer State", 
    "Customer Zip Code", "Latitude", "Longitude", "Location"
).distinct()



# Date Dimension
date_dimension = df.select(
    "Date_ID",
    F.to_timestamp("Transaction Date Time").alias("Full_date"),
    "Year", "Month", F.dayofweek("Transaction Date Time").alias("weekday"),
    "Day", "Hour", "Minute"
).distinct()

# Fraud Dimension
fraud_dimension = df.select(
    "Fraud Flag", "Transaction Risk Score", "Transaction Notes", 
    "Fraud Detection Method", "Fraud_ID_SK"
).distinct()

# Merchant Dimension
merchant_dimension = df.select("Merchant Name","Merchant ID").distinct()


# Transaction Fact Table
transaction_fact = df.select(
    "Transaction ID", "Customer ID", "Merchant ID", "Transaction Amount", 
    "Transaction Type", "Transaction Status", "Transaction Channel", 
    "Response Time", "POS Entry Mode", "Recurring Transaction Flag", 
    "Loyalty Points Earned", "Currency", "Account Balance", 
    "Remaining Balance", "IP Address","Date_ID", "Fraud_ID_SK"
)


##########################################################################################
output_base_path = "s3://datalake-bucket-1212/Cleaned_Data_Staging/" 

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