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
    "knowledge_aggregated",
    "learn_best_aggregated",
    "learning_style_aggregated",
    "confidence_aggregated",
    "access_aggregated",
    "strengths_aggregated",
    "weaknesses_aggregated",
    "training_freq_aggregated",
    "training_time_aggregated",
    "training_when_aggregated",
    "training_length_aggregated",
    "relevance_aggregated",
    "self_care",
    "self_care_aggregated",
    "define_success",
    "define_success_aggregated",
    "confidence_providing_aggregated"
]

for t in tables:
    df = spark.read.table(t).toPandas()
    df.to_csv(f"/lakehouse/default/Files/exports/{t}.csv", index=False, encoding="utf-8-sig")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
