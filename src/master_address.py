
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
        {s.mst_address_link_schema}
    );
""")


# get updated address records from persist
(
    spark.readStream
    .format("delta")
    # .option("startingVersion", 0)
    .table(SRC_TABLE)
    .createOrReplaceTempView("adr_persist_updates") 
)

def process_batch(df, batch_id):     
    df.createOrReplaceTempView("micro_batch_updates")
    spark_session = df.sparkSession

    # pull all records that also have address records in main table 
    adr_persist_full_df = (
        spark_session.sql(f"""
            WITH master_with_child_update AS (
                SELECT mst.master_address_identifier_text 
                FROM {TGT_TABLE} mst
                INNER JOIN micro_batch_updates ad 
                    ON ad.user_address_system_number = mst.user_address_system_number
            ), child_from_master AS (
                SELECT mst.user_address_system_number 
                FROM {TGT_TABLE} mst
                INNER JOIN master_with_child_update mch 
                    ON mch.master_address_identifier_text = mst.master_address_identifier_text
            )
            SELECT adr.* FROM {SRC_TABLE} adr
            INNER JOIN child_from_master cfm 
                ON cfm.user_address_system_number = adr.user_address_system_number
            UNION 
            SELECT * FROM micro_batch_updates
        """)
    )

    adr_persist_full_df.createOrReplaceTempView("adr_persist_full")

    # roll all records up into one, pull winning record
    adr_master_df = (
        spark_session.sql(f"""
            WITH cte AS (
                SELECT 
                    user_address_system_number, 
                    source_system_update_timestamp,
                    record_logical_delete_indicator, 
                    sql_uuid(CONCAT_WS(',', {", ".join(ADDRESS_COLS)})) as master_address_identifier_text
                FROM adr_persist_full
            ), cte2 AS (
                SELECT
                    *, 
                    ROW_NUMBER() OVER (
                    PARTITION BY master_address_identifier_text
                    ORDER BY record_logical_delete_indicator, source_system_update_timestamp, user_address_system_number
                    ) as rn
                FROM cte
            ) 
            SELECT
                user_address_system_number, 
                source_system_update_timestamp, 
                master_address_identifier_text,
                record_logical_delete_indicator,
                CASE WHEN rn = 1 THEN TRUE else FALSE end as primary_user_address_indicator
            FROM cte2
        """)
    )

    adr_master_df.createOrReplaceTempView("adr_master")

    spark_session.sql(f"""
        MERGE INTO {TGT_TABLE} as tgt
        USING adr_master as src
        ON src.user_address_system_number = tgt.user_address_system_number
        WHEN MATCHED AND src.record_logical_delete_indicator = TRUE THEN DELETE
        WHEN MATCHED AND (
            src.primary_user_address_indicator IS DISTINCT FROM tgt.primary_user_address_indicator
            OR src.master_address_identifier_text IS DISTINCT FROM tgt.master_address_identifier_text
        ) THEN UPDATE SET
            tgt.primary_user_address_indicator = src.primary_user_address_indicator,
            tgt.master_address_identifier_text = src.master_address_identifier_text
        WHEN NOT MATCHED AND src.record_logical_delete_indicator = FALSE THEN INSERT (
            master_address_identifier_text, 
            user_address_system_number, 
            primary_user_address_indicator
        ) VALUES (
           src.master_address_identifier_text, 
           src.user_address_system_number, 
           src.primary_user_address_indicator
        )
    """)

query = (
    spark.table("adr_persist_updates").writeStream
    .foreachBatch(process_batch)
    .outputMode("update")
    .option("checkpointLocation", CHK_PATH)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()