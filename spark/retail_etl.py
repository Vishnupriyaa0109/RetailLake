from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, hour
from pyspark.sql.functions import sum
# Create Spark Session
spark = SparkSession.builder \
    .appName("RetailLake ETL") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("Spark Session Created Successfully!")
# Read Bronze Layer CSV
df = spark.read.csv(
    "file:///home/vishnupriya_reddy2-9/RetailLake/data/raw/retail_transactions.csv",
    header=True,
    inferSchema=True
)
# Display Dataset Schema
print("\nDataset Schema:")
df.printSchema()

# Count Total Records
print("\nTotal Records:", df.count())

# Display First Five Records
print("\nFirst Five Records:")
df.show(5)
# Check for Missing Values
print("\nMissing Values:")

for column in df.columns:
    print(column, ":", df.filter(df[column].isNull()).count())

# Create Revenue Column
df = df.withColumn("Revenue", col("Quantity") * col("Price"))

print("\nRevenue Column Created Successfully!")
df.select("Quantity", "Price", "Revenue").show(5)
# Duplicate Detection
total_records = df.count()
unique_records = df.dropDuplicates().count()

print("\nDuplicate Analysis")
print("Total Records   :", total_records)
print("Unique Records  :", unique_records)
print("Duplicate Rows  :", total_records - unique_records)
# Remove Duplicate Records
df = df.dropDuplicates()

print("\nDuplicates Removed Successfully!")
print("Records After Cleaning:", df.count())
# Create Date Features
df = df.withColumn("Year", year(col("InvoiceDate"))) \
       .withColumn("Month", month(col("InvoiceDate"))) \
       .withColumn("Day", dayofmonth(col("InvoiceDate"))) \
       .withColumn("Hour", hour(col("InvoiceDate")))

print("\nDate Features Created Successfully!")

df.select(
    "InvoiceDate",
    "Year",
    "Month",
    "Day",
    "Hour"
).show(5)
# Save Silver Layer as Parquet
df.write.mode("overwrite").parquet(
    "file:///home/vishnupriya_reddy2-9/RetailLake/data/processed/retail_silver"
)

print("\nSilver Layer Saved Successfully!")
# Gold Layer - Country Wise Revenue
gold_df = df.groupBy("Country").agg(sum("Revenue").alias("TotalRevenue"))

print("\nTop 10 Countries by Revenue:")
gold_df.orderBy(col("TotalRevenue").desc()).show(10)

# Save Gold Layer
gold_df.write.mode("overwrite").parquet(
    "file:///home/vishnupriya_reddy2-9/RetailLake/data/processed/retail_gold"
)

print("Gold Layer Saved Successfully!")
