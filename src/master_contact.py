
import uuid
import hashlib

from pyspark.sql import Window, functions as F
from pyspark.sql.types import StringType
from utils.utils import get_config
from utils import schemas as s
import pandas as pd

config = get_config()
target_schema = config.get("target_schema")
target_checkpoint_volume_path = config.get("target_checkpoint_volume_path")

SRC_TABLE = f"{target_schema}.slv_user_address"
TGT_TABLE = f"{target_schema}.mst_address"
CHK_PATH = f"{target_checkpoint_volume_path}/master_address_cp"

ADDRESS_COLS = [
    "street_number",
    "street_predirectional",
    "street_name",
    "street_post_type",
    "street_postdirectional",
    # "unit_type",
    # "unit_identifier",
    # "post_office_box",
    # "building_name",
    "place_name",
    "political_division_code",
    "country_code",
    "postal_code",
]

UUID_NAMESPACE = uuid.UUID("6f1c2a3e-8b47-4d5e-9a10-2c7e5b9d4f31")

"""
Match key deterministically
"""
def uuid5(key:str|None) -> str:
    if key is None: 
        return key
    return str(uuid.uuid5(UUID_NAMESPACE, key))

spark.udf.register("sql_uuid", uuid5, StringType())


spark.sql(f"""USE {target_schema}""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TGT_TABLE} (
        {s.mst_contact_link_schema}
    );
""")

