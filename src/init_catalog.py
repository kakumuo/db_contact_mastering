import os

catalog_name = "workspace"
schema_name = "contact_mastering"
volume_name = "data"

try:
    catalog_name = dbutils.widgets.get("catalog")
    schema_name = dbutils.widgets.get("schema")
    volume_name = dbutils.widgets.get("volume")
except: 
    pass

target_schema = f"{catalog_name}.{schema_name}"
target_volume = f"{catalog_name}.{schema_name}.{volume_name}"
volume_path_base = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
target_volume_path = f"{volume_path_base}/files"
target_checkpoint_volume_path = f"{volume_path_base}/checkpoints"

dbutils.widgets.text("target_schema", target_schema, "Target Schema")
dbutils.widgets.text("target_volume", target_volume, "Target Volume")
dbutils.widgets.text("target_volume_path", target_volume_path, "Target Volume Path")
dbutils.widgets.text("target_checkpoint_volume_path", target_checkpoint_volume_path, "Target Checkpoint Volume Path")

# create catalog and volume if not present
spark.sql(f"CREATE DATABASE IF NOT EXISTS {target_schema};")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {target_volume};")

# create directories
os.makedirs(target_volume_path, exist_ok=True)
os.makedirs(target_checkpoint_volume_path, exist_ok=True)
