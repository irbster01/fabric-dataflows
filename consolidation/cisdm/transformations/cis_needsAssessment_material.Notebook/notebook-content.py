# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "49b28ef6-6017-4b73-8e5a-61dab4008c9a",
# META       "default_lakehouse_name": "VOA_Nexus",
# META       "default_lakehouse_workspace_id": "4241c9c7-5818-4c56-9220-faf632741770",
# META       "known_lakehouses": [
# META         {
# META           "id": "49b28ef6-6017-4b73-8e5a-61dab4008c9a"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import pandas as pd

tables = [
    "knowledge",
    "learn_best",
    "learning_style",
    "confidence",
    "access",
    "strengths_needs_assessment",
    "weaknesses",
    "training_freq",
    "training_time",
    "training_when",
    "training_length",
    "relevance",
    "confidence_providing"
]

for t in tables:
    df = spark.read.table(t).toPandas()
    df.to_csv(f"/lakehouse/default/Files/exports/{t}.csv", index=False, encoding="utf-8-sig")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
