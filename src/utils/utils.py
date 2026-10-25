def get_config():
    catalog_name = "workspace"
    schema_name = "contact_mastering"
    volume_name = "data"

    try:
        catalog_name = dbutils.widgets.get("catalog")
        schema_name = dbutils.widgets.get("schema")
        volume_name = dbutils.widgets.get("volume")
    except:
        pass

    config = {
        "catalog_name": catalog_name,
        "schema_name": schema_name,
        "volume_name": volume_name,
    }

    additional_config = get_additional_config(catalog_name, schema_name, volume_name)
    return {**config, **additional_config}

def get_additional_config(catalog_name="workspace", schema_name="contact_mastering", volume_name="data"):
    target_schema = f"{catalog_name}.{schema_name}"
    target_volume = f"{catalog_name}.{schema_name}.{volume_name}"
    volume_path_base = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
    target_volume_path = f"{volume_path_base}/files"
    target_checkpoint_volume_path = f"{volume_path_base}/checkpoints"

    try:
        target_schema = dbutils.widgets.get("target_schema") or target_schema
        target_volume = dbutils.widgets.get("target_volume") or target_volume
        target_volume_path = dbutils.widgets.get("target_volume_path") or target_volume_path
        target_checkpoint_volume_path = dbutils.widgets.get("target_checkpoint_volume_path") or target_checkpoint_volume_path
    except:
        pass

    return {
        "target_schema": target_schema,
        "target_volume": target_volume,
        "target_volume_path": target_volume_path,
        "target_checkpoint_volume_path": target_checkpoint_volume_path
    }