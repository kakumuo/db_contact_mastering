from pyspark.sql.functions import col, count, count_if
from utils.utils import get_config
from utils import schemas as s

# Get configuration from utils.py
config = get_config()
target_schema = config.get("target_schema")
target_volume_path = config.get("target_volume_path")
target_checkpoint_volume_path = config.get("target_checkpoint_volume_path")

user_table_name = f"{target_schema}.raw_user"
delete_table_name = f"{target_schema}.raw_delete_request"

spark.sql(f"USE {target_schema}")


spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {user_table_name} (
        {s.user_raw_schema}
    );
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {delete_table_name} (
        {s.delete_request_raw_schema}
    );
""")


def ingest_user_raw(): 
    raw_df = spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .option("multiline", True) \
        .option("pathGlobFilter", "user_*.json") \
        .schema(s.user_raw_file_schema) \
        .load(f"{target_volume_path}")

    query = raw_df.writeStream \
        .format("delta") \
        .option("checkpointLocation", f"{target_checkpoint_volume_path}/raw_user_cp") \
        .trigger(availableNow=True) \
        .toTable(user_table_name)
    
    query.awaitTermination()

ingest_user_raw()

def ingest_delete_request(): 
    raw_df = spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .option("multiline", True) \
        .option("pathGlobFilter", "delete_request_*.json") \
        .schema(s.delete_request_raw_file_schema) \
        .load(f"{target_volume_path}")

    query = raw_df.writeStream \
        .format("delta") \
        .option("checkpointLocation", f"{target_checkpoint_volume_path}/raw_delete_request_cp") \
        .trigger(availableNow=True) \
        .toTable(delete_table_name)
    
    query.awaitTermination()

ingest_delete_request()