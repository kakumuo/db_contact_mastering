def get_config():
    catalog_name = dbutils.widgets.get("catalog")
    schema_name = dbutils.widgets.get("schema")
    volume_name = dbutils.widgets.get("volume")

    if not catalog_name or not schema_name or not volume_name:
        return {catalog_name, schema_name, volume_name}

    additional_config = get_additional_config()
    return {**additional_config, **{catalog_name, schema_name, volume_name}}

def get_additional_config():
    target_schema = dbutils.widgets.get("target_schema")
    target_volume = dbutils.widgets.get("target_volume")
    target_volume_path = dbutils.widgets.get("target_volume_path")
    target_checkpoint_volume_path = dbutils.widgets.get("target_checkpoint_volume_path")

    if not target_schema or not target_volume or not target_volume_path or not target_checkpoint_volume_path:
        return {}

    return {
        "target_schema": target_schema,
        "target_volume": target_volume,
        "target_volume_path": target_volume_path,
        "target_checkpoint_volume_path": target_checkpoint_volume_path
    }