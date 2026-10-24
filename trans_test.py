from pyspark.sql import functions as F
import pandas as pd

catalog_name = "workspace"
schema_name = "contact_mastering"
volume_name = "data_raw"

try:
    catalog_name = dbutils.widgets.get("catalog")
    schema_name = dbutils.widgets.get("schema")
    volume_name = dbutils.widgets.get("volume")
except: 
    pass

target_schema = f"{catalog_name}.{schema_name}"
target_volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"



user_profile_df = spark.read.table(f"{target_schema}.user_profile").alias("usr")
user_delete_raw_df = spark.readStream.table(f"{target_schema}.delete_request_raw").alias("raw")

user_delete_df = user_delete_raw_df\
    .join(user_profile_df.alias("usr"), F.col("usr.user_profile_name") == F.col("raw.username"))\
    .select(
        "usr.user_profile_system_number", 
        F.lit(True).alias("user_profile_delete_indicator"),
        F.col("raw.requested_at").alias("user_profile_delete_date"),
        F.col("raw.requested_at").alias("source_system_update_timestamp")
    )

def process_batch(batch_df, batch_id):
    print(f"Batch {batch_id} processed {batch_df.count()} records.")

query = user_delete_df.writeStream\
    .foreachBatch(process_batch) \
    .option("checkpointLocation", f"{target_volume_path}/user_profile_delete_checkpoint") \
    .trigger(availableNow=True) \
    .start()

query.awaitTermination()
 