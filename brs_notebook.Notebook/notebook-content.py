# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f12b0505-9183-40c7-a6d8-3b9e2314ca9a",
# META       "default_lakehouse_name": "Accounting",
# META       "default_lakehouse_workspace_id": "4241c9c7-5818-4c56-9220-faf632741770",
# META       "known_lakehouses": [
# META         {
# META           "id": "f12b0505-9183-40c7-a6d8-3b9e2314ca9a"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# Step 1: Load Tables from Lakehouse

# CELL ********************

import pandas as pd

# Load tables from Accounting Lakehouse
df_prog = spark.read.table("credible_brs.brs_balance_by_prog").toPandas()
df_payer = spark.read.table("credible_brs.brs_balance_by_Payer").toPandas()
df_status = spark.read.table("credible_brs.`BRS by Review Status`").toPandas()
df_servType = spark.read.table("credible_brs.brs_balance_by_servType").toPandas()
df_balance = spark.read.table("credible_brs.brs_w_balance").toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write Excel file locally
local_path = "/lakehouse/default/Files/brs_balance_export.xlsx"

with pd.ExcelWriter(local_path) as writer:
    df_prog.to_excel(writer, sheet_name="Balance by Program", index=False)
    df_payer.to_excel(writer, sheet_name="Balance by Payer", index=False)
    df_status.to_excel(writer, sheet_name="Balance by Status", index=False)
    df_servType.to_excel(writer, sheet_name="Balance by Service Type", index=False)
    df_balance.to_excel(writer, sheet_name="Services with Balance", index=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
