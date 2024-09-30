import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
###################################################################################################################loadDatafroms3
# Load each CSV file separately
# Load Transaction Fact Table
Transaction_Fact_node = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://datalake-test12121/Cleaned_Data_Staging/transaction_fact/"], "recurse": True},
    transformation_ctx="Transaction_Fact_node"
)

# Load Date Dimension Table
Date_Dim_node = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://datalake-test12121/Cleaned_Data_Staging/date_dimension/"], "recurse": True},
    transformation_ctx="Date_Dim_node"
)

# Load Merchant Dimension Table
Merchant_DIM_node = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://datalake-test12121/Cleaned_Data_Staging/merchant_dimension/"], "recurse": True},
    transformation_ctx="Merchant_DIM_node"
)

# Load Location Dimension Table
Location_Dim_node = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://datalake-test12121/Cleaned_Data_Staging/location_dimension/"], "recurse": True},
    transformation_ctx="Location_Dim_node"
)

# Load Customer Dimension Table
Customer_DIM_node = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://datalake-test12121/Cleaned_Data_Staging/customer_dimension/"], "recurse": True},
    transformation_ctx="Customer_DIM_node"
)

# Load fraud Dimension Table
Fraud_DIM_node = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://datalake-test12121/Cleaned_Data_Staging/fraud_dimension/"], "recurse": True},
    transformation_ctx="Fraud_DIM_node"
)
#####################################################################################################################################################



Date_Dim_node = ApplyMapping.apply(
    frame=Date_Dim_node,
    mappings=[
        ("Full_date", "string", "full_date", "timestamp"),
        ("Date_ID", "string", "date_id", "bigint"),
        ("Year", "string", "year", "int"),
        ("Month", "string", "month", "int"),
        ("weekday", "string", "weekday", "int"),
        ("Day", "string", "day", "int"),
        ("Hour", "string", "hour", "int"),
        ("Minute", "string", "minute", "int")
    ],
    transformation_ctx="Date_Dim_node"
)

Merchant_DIM_node = ApplyMapping.apply(
    frame=Merchant_DIM_node,
    mappings=[
        ("Merchant_ID", "string", "merchant_id", "bigint"),
        ("Merchant Name", "string", "merchant_name", "string"),
    ],
    transformation_ctx="Merchant_DIM_node"
)

Location_Dim_node = ApplyMapping.apply(
    frame=Location_Dim_node,
    mappings=[
        ("Latitude", "string", "latitude", "float"),
        ("Longitude", "string", "longitude", "float"),
        ("Location", "string", "location", "string"),
        ("Customer State", "string", "customer_state", "string"),
        ("Customer Zip Code", "string", "customer_zip_code", "string"),
        ("City", "string", "city", "string"),
        ("Address", "string", "address", "string"),
        ("Location_ID_SK", "string", "location_id_sk", "int"),
        
    ],
    transformation_ctx="Location_Dim_node"
)

Customer_DIM_node = ApplyMapping.apply(
    frame=Customer_DIM_node,
    mappings=[
        ("Bank Name", "string", "bank_name", "string"),
        ("Birth Date", "string", "birth_date", "date"),
        ("Card Type", "string", "card_type", "string"),
        ("Cardholder's Gender", "string", "cardholder_gender", "string"),
        ("Country", "string", "country", "string"),
        ("Customer ID", "string", "customer_id", "string"),
        ("First Name", "string", "first_name", "string"),
        ("Income Bracket", "string", "income_bracket", "string"),
        ("Last Name", "string", "last_name", "string"),
        ("Phone Number","string","phone_number","string"),
        ("Card Issuer Country","string","card_issuer_country","string"),
        ("Cardholder's Age","string","cardholder_age","string"),
        ("Card Expiration Date","string","card_expiration_date","string"),
        ("Location_ID_SK", "string", "location_id_sk", "int")
        
    ],
     transformation_ctx="Customer_DIM_node"    
)

Fraud_DIM_node = ApplyMapping.apply(
    frame=Fraud_DIM_node,
    mappings=[
        ("Fraud Flag", "string", "fraud_flag", "boolean"),
        ("Transaction Risk Score", "string", "fraud_risk_score", "float"),
        ("Transaction Notes", "string", "transaction_notes", "string"),
        ("Fraud Detection Method", "string", "fraud_detection_method", "string"),
        ("Fraud_ID_SK", "string", "fraud_id_sk", "bigint")
    ],
    transformation_ctx="Fraud_DIM_node"
)




# Apply mappings for different dimensions and facts
Transaction_Fact_node = ApplyMapping.apply(
    frame=Transaction_Fact_node,
    mappings=[
        ("Account Balance", "string", "account_balance", "double"),
        ("Currency", "string", "currency", "string"),
        ("Loyalty Points Earned", "string", "loyalty_points_earned", "int"),
        ("Recurring Transaction Flag", "string", "recurring_transaction_flag", "boolean"),
        ("Remaining Balance", "string", "remaining_balance", "double"),
        ("Transaction Amount", "string", "transaction_amount", "float"),
        ("Transaction Channel", "string", "transaction_channel", "string"),
        ("Transaction ID", "string", "transaction_id", "string"),
        ("Transaction Status", "string", "transaction_status", "string"),
        ("Transaction Type", "string", "transaction_type", "string"),
        ("Response Time", "string", "response_time", "float"),
        ("POS Entry Mode", "string", "pos_entry_mode", "string"),
        ("IP Address", "string", "ip_address", "string"),
        ("Customer ID", "string", "customer_id", "string"),
        ("Date_ID", "string", "date_id", "bigint"),
        ("Merchant_ID", "string", "merchant_id", "bigint"),
        ("Fraud_ID_SK", "string", "fraud_id_sk", "bigint")
        
    ],
    transformation_ctx="Transaction_Fact_node"
)

####################################################################################################################WRITEINTOREDSHIFT
# Write the Date Dimension Table to Redshift
glueContext.write_dynamic_frame.from_options(
    frame=Date_Dim_node,
    connection_type="redshift",
    connection_options={
        "url": "jdbc:redshift://redshift-cluster-1.cfld3nz4rrvw.eu-north-1.redshift.amazonaws.com:5439/dev",
        "user": "awsuser",
        "password": "Dataeng24",
        "dbtable": "dwh.date_dimension",
        "redshiftTmpDir": "s3://aws-glue-assets-654654362485-eu-north-1/temporary/"
    },
    transformation_ctx="dwhDate_Dim_node"
)

# Write the Merchant Dimension Table to Redshift
glueContext.write_dynamic_frame.from_options(
    frame=Merchant_DIM_node,
    connection_type="redshift",
    connection_options={
        "url": "jdbc:redshift://redshift-cluster-1.cfld3nz4rrvw.eu-north-1.redshift.amazonaws.com:5439/dev",
        "user": "awsuser",
        "password": "Dataeng24",
        "dbtable": "dwh.merchant_dimension",
        "redshiftTmpDir": "s3://aws-glue-assets-654654362485-eu-north-1/temporary/"
    },
    transformation_ctx="dwhMerchant_DIM_node"
)

# Write the Location Dimension Table to Redshift
glueContext.write_dynamic_frame.from_options(
    frame=Location_Dim_node,
    connection_type="redshift",
    connection_options={
        "url": "jdbc:redshift://redshift-cluster-1.cfld3nz4rrvw.eu-north-1.redshift.amazonaws.com:5439/dev",
        "user": "awsuser",
        "password": "Dataeng24",
        "dbtable": "dwh.location_dimension",
        "redshiftTmpDir": "s3://aws-glue-assets-654654362485-eu-north-1/temporary/"
    },
    transformation_ctx="dwhLocation_Dim_node"
)

# Write the Customer Dimension Table to Redshift
glueContext.write_dynamic_frame.from_options(
    frame=Customer_DIM_node,
    connection_type="redshift",
    connection_options={
        "url": "jdbc:redshift://redshift-cluster-1.cfld3nz4rrvw.eu-north-1.redshift.amazonaws.com:5439/dev",
        "user": "awsuser",
        "password": "Dataeng24",
        "dbtable": "dwh.customer_dimension",
        "redshiftTmpDir": "s3://aws-glue-assets-654654362485-eu-north-1/temporary/"
    },
    transformation_ctx="dwhCustomer_DIM_node"
)

# Write the fraud Dimension Table to Redshift
glueContext.write_dynamic_frame.from_options(
    frame=Fraud_DIM_node,
    connection_type="redshift",
    connection_options={
        "url": "jdbc:redshift://redshift-cluster-1.cfld3nz4rrvw.eu-north-1.redshift.amazonaws.com:5439/dev",
        "user": "awsuser",
        "password": "Dataeng24",
        "dbtable": "dwh.fraud_dimension",
        "redshiftTmpDir": "s3://aws-glue-assets-654654362485-eu-north-1/temporary/"
    },
    transformation_ctx="dwhFraud_DIM_node"
)

# Write the Transaction Fact Table to Redshift
glueContext.write_dynamic_frame.from_options(
    frame=Transaction_Fact_node,
    connection_type="redshift",
    connection_options={
        "url": "jdbc:redshift://redshift-cluster-1.cfld3nz4rrvw.eu-north-1.redshift.amazonaws.com:5439/dev",
        "user": "awsuser",
        "password": "Dataeng24",
        "dbtable": "dwh.transaction_fact_table",
        "redshiftTmpDir": "s3://aws-glue-assets-654654362485-eu-north-1/temporary/"
    },
    transformation_ctx="dwhTransaction_Fact_node"
)

job.commit()