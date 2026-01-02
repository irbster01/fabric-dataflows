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
import re, uuid

# Fabric notebooks usually have this:
from notebookutils import mssparkutils

tables = [
    "Client",
    "Disabilities",
    "EmploymentEducation",
    "Enrollment",
    "Exit",
    "Export",
    "Funder",
    "HealthAndDV",
    "IncomeBenefits",
    "Project",
    "ProjectCoC",
    "Services"
]

out_dir = "Files/exports_csv"
mssparkutils.fs.mkdirs(out_dir)

def safe_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", name)

def export_table_as_single_csv(table_name: str):
    safe = safe_name(table_name)
    tmp_dir = f"{out_dir}/_tmp_{safe}_{uuid.uuid4().hex}"
    final_csv = f"{out_dir}/{safe}.csv"

    # Read
    df = spark.table(f"`{table_name}`")  # backticks help if you ever have weird names

    # Write to temp folder (Spark always writes a folder)
    (df.coalesce(1)
      .write.mode("overwrite")
      .option("header", True)
      .option("quote", '"')
      .option("escape", '"')
      .csv(tmp_dir))

    # Find the single part file
    tmp_files = mssparkutils.fs.ls(tmp_dir)
    part_files = [f.path for f in tmp_files if f.name.startswith("part-") and f.name.endswith(".csv")]
    if not part_files:
        raise Exception(f"No part CSV found in {tmp_dir}. Files: {[f.name for f in tmp_files]}")

    part_csv = part_files[0]

    # Remove existing output file if present
    try:
        mssparkutils.fs.rm(final_csv, True)
    except:
        pass

    # Move part file to final name (no subfolders)
    # If mv gives you trouble in your environment, swap to cp+rm below.
    mssparkutils.fs.mv(part_csv, final_csv)

    # Cleanup temp folder (removes _SUCCESS etc.)
    mssparkutils.fs.rm(tmp_dir, True)

for t in tables:
    export_table_as_single_csv(t)

print(f"Done. Check Lakehouse > Files > exports_csv/ (should be 12 flat *.csv files)")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
