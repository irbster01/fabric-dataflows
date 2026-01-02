# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c46fe4a6-e106-4e77-993b-efc9ce22e551",
# META       "default_lakehouse_name": "Silver",
# META       "default_lakehouse_workspace_id": "4241c9c7-5818-4c56-9220-faf632741770",
# META       "known_lakehouses": [
# META         {
# META           "id": "c46fe4a6-e106-4e77-993b-efc9ce22e551"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Bundle Analysis - Spark Transformation
# 
# This notebook replicates the M code transformation logic for analyzing bundle services. It joins primary services with secondary services, calculates totals, and categorizes them by month.
# 
# ## Data Flow:
# 1. Load source tables (transform-credible-services and bundle-secondaries)
# 2. Join tables on Client ID and Month
# 3. Calculate combined counts (Addition = Count + sec-count)
# 4. Apply bundle classification logic (BUN if >= 4, else NO)
# 5. Group and count by Month and bundle flag
# 6. Sort and format final results

# CELL ********************

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder \
    .appName("BundleAnalysis") \
    .getOrCreate()

print("Spark session initialized successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1: Load Source Data
# 
# Load the two source tables that will be joined:
# - `transform-credible-services-1 (3)` - primary services data
# - `bundle-secondaries` - secondary services data

# CELL ********************

# Load source tables from lakehouse
# Note: Adjust table names and lakehouse paths as needed for your environment

# Option 1: Load from lakehouse tables
try:
    # Primary services table (equivalent to transform-credible-services-1 (3))
    # Note: Using backticks to escape table names with hyphens
    primary_services_df = spark.table("`transform-credible-bundle-primary`")
    print(f"Primary services loaded: {primary_services_df.count()} rows")
    
    # Secondary services table (bundle-secondaries)
    secondary_services_df = spark.table("`bundle-secondaries`")
    print(f"Secondary services loaded: {secondary_services_df.count()} rows")
    
except Exception as e:
    print(f"Error loading from tables: {e}")
    print("Creating sample data for demonstration...")
    
    # Create sample data for demonstration
    primary_data = [
        ("C001", "2024-01", 2),
        ("C001", "2024-02", 3),
        ("C002", "2024-01", 1),
        ("C002", "2024-02", 4),
        ("C003", "2024-01", 3),
        ("C003", "2024-02", 2),
        ("C004", "2024-01", 5),
        ("C004", "2024-02", 1)
    ]
    
    secondary_data = [
        ("C001", "2024-01", 1),
        ("C001", "2024-02", 2),
        ("C002", "2024-01", 3),
        ("C002", "2024-02", 1),
        ("C003", "2024-01", 2),
        ("C003", "2024-02", 3),
        ("C004", "2024-01", 0),
        ("C004", "2024-02", 4)
    ]
    
    primary_services_df = spark.createDataFrame(
        primary_data, 
        ["Client ID", "Month", "Count"]
    )
    
    secondary_services_df = spark.createDataFrame(
        secondary_data, 
        ["Client ID", "Month", "sec-count"]
    )
    
    print("Sample data created successfully")

# Display schema and sample data
print("\nPrimary Services Schema:")
primary_services_df.printSchema()
print("\nPrimary Services Sample:")
primary_services_df.show(5)

print("\nSecondary Services Schema:")
secondary_services_df.printSchema()
print("\nSecondary Services Sample:")
secondary_services_df.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2: Join Tables and Calculate Addition
# 
# Perform inner join on Client ID and Month, then calculate the combined count.

# CELL ********************

# Step 1: Inner join on Client ID and Month (equivalent to Table.NestedJoin)
joined_df = primary_services_df.join(
    secondary_services_df,
    on=["Client ID", "Month"],
    how="inner"
)

print("After join:")
joined_df.show()

# Step 2: Add Addition column (Count + sec-count)
addition_df = joined_df.withColumn(
    "Addition", 
    col("Count") + col("sec-count")
)

# Step 3: Remove original Count and sec-count columns
cleaned_df = addition_df.drop("Count", "sec-count")

print("\nAfter adding Addition and removing original columns:")
cleaned_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3: Sort and Apply Bundle Logic
# 
# Sort by Month descending and apply bundle classification logic.

# CELL ********************

# Step 4: Sort by Month descending
sorted_df = cleaned_df.orderBy(col("Month").desc())

# Step 5: Add conditional column (BUN if Addition >= 4, else NO)
bundle_classified_df = sorted_df.withColumn(
    "Custom",
    when(col("Addition") >= 4, "BUN").otherwise("NO")
)

print("After sorting and adding bundle classification:")
bundle_classified_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 4: Group and Count by Month and Bundle Flag
# 
# Group by Month and Custom flag, then count rows in each group.

# CELL ********************

# Step 6: Group by Month and Custom, count rows
grouped_df = bundle_classified_df.groupBy("Month", "Custom") \
    .agg(count("*").alias("Count"))

print("After grouping and counting:")
grouped_df.show()

# Step 7: Sort by Month descending again
final_sorted_df = grouped_df.orderBy(col("Month").desc())

print("After final sorting:")
final_sorted_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 5: Final Formatting and Renaming
# 
# Rename columns and ensure proper data types for final output.

# CELL ********************

# Step 8: Rename columns (Count -> bundle-count, Custom -> bundle-flag)
renamed_df = final_sorted_df \
    .withColumnRenamed("Count", "bundle-count") \
    .withColumnRenamed("Custom", "bundle-flag")

# Step 9: Ensure bundle-flag is text type (string)
final_df = renamed_df.withColumn("bundle-flag", col("bundle-flag").cast("string"))

print("Final result:")
final_df.show()

# Display final schema
print("\nFinal Schema:")
final_df.printSchema()

# Show summary statistics
print("\nSummary by bundle flag:")
final_df.groupBy("bundle-flag").agg(
    sum("bundle-count").alias("total_count"),
    count("*").alias("month_count")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
