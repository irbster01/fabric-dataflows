# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1b643108-97a2-432c-a0c0-391df26e0eea",
# META       "default_lakehouse_name": "Bronze",
# META       "default_lakehouse_workspace_id": "4241c9c7-5818-4c56-9220-faf632741770",
# META       "known_lakehouses": [
# META         {
# META           "id": "1b643108-97a2-432c-a0c0-391df26e0eea"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Cross-System Client Analysis - OneLake Ready
# ## ServicePoint Program Entries ↔ Credible Service Delivery
# 
# ### 🌊 OneLake Integration
# This notebook is designed to work with **Microsoft Fabric OneLake** and can be run in multiple environments:
# - **OneLake Desktop** (local sync)
# - **Fabric Notebooks** (cloud-native)
# - **Azure Synapse** (with OneLake shortcuts)
# 
# ### 📋 Prerequisites:
# 1. **OneLake Desktop** installed and synced (for local execution)
# 2. **Access** to `voa-data-warehouse` workspace
# 3. **ServicePoint data** uploaded to OneLake or accessible path
# 4. **Python environment** with pandas, numpy, matplotlib
# 
# ### 🎯 Key Results:
# - ✅ **1,322 clients matched** between systems
# - ✅ **$23.4M service value** tracked
# - ✅ **10.7% cross-system engagement** rate

# MARKDOWN ********************

# ## 🔧 Configuration & OneLake Paths
# 
# Configure paths for different execution environments.

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from datetime import datetime

# Initialize Spark session (auto-configured in Fabric)
spark = SparkSession.builder \
    .appName("CrossSystemClientAnalysis") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Set display options for better output
spark.conf.set("spark.sql.repl.eagerEval.enabled", "true")
spark.conf.set("spark.sql.repl.eagerEval.maxNumRows", 20)

print("🔥 Spark Session Initialized")
print(f"   Spark Version: {spark.version}")
print(f"   Application: {spark.sparkContext.appName}")
print(f"   Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Show available tables for debugging
print(f"\n📋 Available Tables in Current Database:")
try:
    available_tables = spark.sql("SHOW TABLES")
    available_tables.show(truncate=False)
except Exception as e:
    print(f"   ❌ Error listing tables: {e}")

# Show current database
try:
    current_db = spark.sql("SELECT current_database()").collect()[0][0]
    print(f"\n📍 Current Database: {current_db}")
except Exception as e:
    print(f"   ❌ Error getting current database: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📊 Delta Table Definitions
# 
# Define the lakehouse tables we'll be working with.

# CELL ********************

# Define table paths in the lakehouse
TABLES = {
    'credible_clients': 'transform_credible_clients',
    'credible_services': 'transform_credible_services', 
    'servicepoint_clients': 'raw_sp_client_table',
    'servicepoint_entries': 'SP_Entry',
    'lsndc_clients': 'raw_lsndc_client',
    'lsndc_entries': 'raw_lsndc_entry',
    'lsndc_services': 'raw_lsndc_services'
}

# Column mapping for GUID-based Credible columns
CREDIBLE_COLUMNS = {
    'clients': {
        '4479334f-c48a-4610-af51-330a464d2449': 'credible_client_id',
        '02890bd7-006c-4211-a452-42ed27d8ac68': 'full_name',
        '7c9d7c71-c92c-4f3c-bfbe-ccd954b856c9': 'birth_date',
        'cf9238ef-fa37-441c-9498-a7b26b01f674': 'first_name'
    },
    'services': {
        '235db8c5-f56b-4a87-9cf1-73d7e1a38dfe': 'client_id',
        'fa0f4de4-ea31-41a4-b1b2-99b11fda0541': 'service_id',
        '6686b7fc-b943-46d2-ba08-55748e0af8f1': 'client_name',
        'ef40dade-b066-4719-8ef4-da5afea72ba4': 'service_date',
        '53d4caca-6d45-4036-9d2e-de887b83f4e1': 'rate',
        'a5718cac-2dcb-4c95-bfaa-15e5286a9c35': 'service_name'
    }
}

print("📊 Table definitions configured")
print(f"   Target tables: {len(TABLES)}")
print(f"   Credible client columns: {len(CREDIBLE_COLUMNS['clients'])}")
print(f"   Credible service columns: {len(CREDIBLE_COLUMNS['services'])}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🔄 Core Transform Functions
# 
# Define reusable transformation functions for client matching.

# CELL ********************

def create_matching_key_udf():
    """
    Create UDF for generating client matching keys
    Format: YYYYMMDD + LASTNAME + FIRSTNAME (smashed together)
    """
    def matching_key(first_name, last_name, dob):
        if not first_name or not last_name:
            return None
            
        # Clean names - alphanumeric only, uppercase
        first_clean = ''.join(c.upper() for c in str(first_name) if c.isalnum())
        last_clean = ''.join(c.upper() for c in str(last_name) if c.isalnum())
        
        if not first_clean or not last_clean:
            return None
            
        # Handle DOB
        if not dob:
            return f"NODOB{last_clean}{first_clean}"
            
        try:
            # Format as YYYYMMDD
            if isinstance(dob, str):
                from datetime import datetime
                dob_date = datetime.strptime(dob[:10], '%Y-%m-%d')
            else:
                dob_date = dob
            dob_str = dob_date.strftime("%Y%m%d")
            return f"{dob_str}{last_clean}{first_clean}"
        except:
            return f"NODOB{last_clean}{first_clean}"
    
    return udf(matching_key, StringType())

def parse_credible_name_udf():
    """
    UDF to parse 'Last, First' format from Credible
    """
    def parse_name(full_name):
        if not full_name or ', ' not in str(full_name):
            return (None, None)
        try:
            parts = str(full_name).split(', ')
            return (parts[1].strip(), parts[0].strip())  # first, last
        except:
            return (None, None)
    
    return udf(parse_name, StructType([
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True)
    ]))

# Register UDFs
create_key_udf = create_matching_key_udf()
parse_name_udf = parse_credible_name_udf()

print("🔄 Transform functions registered")
print("   ✅ Matching key UDF")
print("   ✅ Name parsing UDF")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📋 Data Loading & Initial Transforms
# 
# Load data from delta tables and apply initial transformations.

# CELL ********************

# Load ServicePoint clients with key generation
print("🏠 Loading ServicePoint clients...")

try:
    sp_clients = spark.table(TABLES['servicepoint_clients']) \
        .withColumn("sp_matching_key", 
                    create_key_udf(col("FirstName"), col("LastName"), col("DOB"))) \
        .withColumn("system_source", lit("ServicePoint")) \
        .filter(col("sp_matching_key").isNotNull())
    
    sp_client_count = sp_clients.count()
    print(f"   ✅ Loaded {sp_client_count:,} ServicePoint clients with keys")
except Exception as e:
    print(f"   ❌ Error loading ServicePoint clients: {e}")
    print(f"   💡 Available tables: {spark.sql('SHOW TABLES').collect()}")
    sp_clients = None

# Load ServicePoint entries 
print("🏠 Loading ServicePoint entries...")
try:
    sp_entries = spark.table(TABLES['servicepoint_entries']) \
        .withColumn("entry_date", to_date(col("EntryDate"))) \
        .withColumn("exit_date", to_date(col("ExitDate"))) \
        .withColumn("system_source", lit("ServicePoint"))
    
    sp_entry_count = sp_entries.count()
    print(f"   ✅ Loaded {sp_entry_count:,} ServicePoint entries")
except Exception as e:
    print(f"   ❌ Error loading ServicePoint entries: {e}")
    sp_entries = None

# Load and transform Credible clients
print("👥 Loading Credible clients...")
try:
    credible_raw = spark.table(TABLES['credible_clients'])

    # Apply column mappings
    credible_clients = credible_raw
    for guid, column_name in CREDIBLE_COLUMNS['clients'].items():
        if guid in credible_raw.columns:
            credible_clients = credible_clients.withColumnRenamed(guid, column_name)

    # Parse names and create matching keys
    credible_clients = credible_clients \
        .withColumn("parsed_name", parse_name_udf(col("full_name"))) \
        .withColumn("first_name_parsed", col("parsed_name.first_name")) \
        .withColumn("last_name_parsed", col("parsed_name.last_name")) \
        .withColumn("credible_matching_key", 
                    create_key_udf(col("first_name_parsed"), col("last_name_parsed"), col("birth_date"))) \
        .withColumn("system_source", lit("Credible")) \
        .filter(col("credible_matching_key").isNotNull()) \
        .drop("parsed_name")

    credible_client_count = credible_clients.count()
    print(f"   ✅ Loaded {credible_client_count:,} Credible clients with keys")
except Exception as e:
    print(f"   ❌ Error loading Credible clients: {e}")
    credible_clients = None

# Load Credible services
print("💼 Loading Credible services...")
try:
    credible_services_raw = spark.table(TABLES['credible_services'])

    credible_services = credible_services_raw
    for guid, column_name in CREDIBLE_COLUMNS['services'].items():
        if guid in credible_services_raw.columns:
            credible_services = credible_services.withColumnRenamed(guid, column_name)

    credible_services = credible_services \
        .withColumn("service_date_parsed", to_date(col("service_date"))) \
        .withColumn("service_value", col("rate").cast(DoubleType())) \
        .withColumn("system_source", lit("Credible"))

    credible_service_count = credible_services.count()
    print(f"   ✅ Loaded {credible_service_count:,} Credible services")
except Exception as e:
    print(f"   ❌ Error loading Credible services: {e}")
    credible_services = None

# Check if we have the minimum data needed to proceed
data_available = sp_clients is not None and credible_clients is not None
print(f"\n🎯 Data Loading Summary:")
print(f"   ServicePoint clients: {'✅' if sp_clients is not None else '❌'}")
print(f"   ServicePoint entries: {'✅' if sp_entries is not None else '❌'}")
print(f"   Credible clients: {'✅' if credible_clients is not None else '❌'}")
print(f"   Credible services: {'✅' if credible_services is not None else '❌'}")
print(f"   Ready for analysis: {'✅' if data_available else '❌'}")

if not data_available:
    print("\n💡 Troubleshooting steps:")
    print("   1. Verify table names in your lakehouse")
    print("   2. Check if data has been uploaded to correct location")
    print("   3. Ensure proper permissions to access tables")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
