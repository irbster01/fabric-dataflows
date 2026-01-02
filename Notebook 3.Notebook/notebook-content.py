# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0a6e0b32-26e7-4e4e-891d-833423d2e910",
# META       "default_lakehouse_name": "LSNDC",
# META       "default_lakehouse_workspace_id": "4241c9c7-5818-4c56-9220-faf632741770",
# META       "known_lakehouses": [
# META         {
# META           "id": "0a6e0b32-26e7-4e4e-891d-833423d2e910"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
import re


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
import re

# Put your table names here (as they appear under Lakehouse > Tables)
tables = [
    "Client",
    "Disablilities",
    "EmploymentEducation",
    "Enrollment",
    "Exit",
    "Export",
    "Funder",
    "HealthAndDV",
    "IncomeBenefits",
    "Project",
    "PrjectCoC",
    "Services"
]

# Where to write in the same Lakehouse (Files area)
out_dir = "Files/exports_csv"

def safe_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", name)

for t in tables:
    df = spark.table(t)  # if this fails, use spark.table(f"`{t}`") or include schema/db prefix
    out_path = f"{out_dir}/{safe_name(t)}"

    # Write as a single CSV file per table:
    (df.coalesce(1)
       .write.mode("overwrite")
       .option("header", True)
       .option("quote", '"')
       .option("escape", '"')
       .csv(out_path))

print("Done. Check Lakehouse > Files > exports_csv/")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
