
import uuid
import hashlib

from pyspark.sql import SparkSession, DataFrame, Window, functions as F
from pyspark.sql.types import StringType
from utils.utils import get_config
from utils import schemas as s
import pandas as pd

config = get_config()
target_schema = config.get("target_schema")
target_checkpoint_volume_path = config.get("target_checkpoint_volume_path")

SRC_PROF = f"{target_schema}.slv_user_profile"
SRC_PREF = f"{target_schema}.slv_user_preference"
SRC_ADDR = f"{target_schema}.slv_user_address"
TGT_TABLE = f"{target_schema}.mst_contact"
CHK_PATH = f"{target_checkpoint_volume_path}/master_contact_cp"

MAX_ITER = 100  # safety cap for the label-propagation loop

spark.sql(f"""USE {target_schema}""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TGT_TABLE} (
        {s.mst_contact_link_schema}
    );
""")

# TODO: implement batching logic to capture records that were already processed
def process_batch(batch_df:DataFrame, batch_id:int): 
    batch_df.createOrReplaceTempView("user_profile_updates_mb")
    spark_session:SparkSession = batch_df.sparkSession

    nodes_df = (
        spark_session.sql(f"""
            WITH changed AS (
                SELECT user_profile_system_number
                FROM user_profile_updates_mb
            ),
            affected_masters AS (
                SELECT DISTINCT master_contact_identifier_text
                FROM {TGT_TABLE}
                WHERE user_profile_system_number IN (SELECT user_profile_system_number FROM changed)
            ),
            affected_ids AS (
                SELECT user_profile_system_number FROM changed
                UNION
                SELECT m.user_profile_system_number
                FROM {TGT_TABLE} m
                JOIN affected_masters USING (master_contact_identifier_text)
            )
            SELECT
                p.user_profile_system_number as id,
                p.user_profile_system_number as base,
                LOWER(TRIM(p.electronic_mail_address)) AS email_key,
                LOWER(TRIM(p.telephone_number))        AS phone_key,
                p.source_system_update_timestamp,
                p.user_profile_delete_indicator
            FROM {SRC_PROF} p
            JOIN affected_ids USING (user_profile_system_number)
        """)
    )

    # create edges
    base_df = nodes_df.filter(F.col("user_profile_delete_indicator") == False)
    email_edges_df = (
        base_df.filter(F.col("email_key").isNotNull())
        .withColumn(
            "root",
            F.min("id").over(Window.partitionBy("email_key"))
        )
        .select("id", "root", F.lit("email").alias("source"))
    )
    phone_edges_df = (
        base_df
        .filter(F.col("phone_key").isNotNull())
        .withColumn(
            "root",
            F.min("id").over(Window.partitionBy("phone_key"))
        )
        .select("id", "root", F.lit("phone").alias("source"))
    )
    base2_df = email_edges_df.union(phone_edges_df).distinct().filter(F.col("id") != F.col("root"))

    edges_df = (
        base2_df.select(F.col("id").alias("src"), F.col("root").alias("dst"), "source")
        .union(
            base2_df.select(F.col("root").alias("src"), F.col("id").alias("dst"), "source")
        )
        .distinct()
    )

    # traverse
    # TODO: should use GraphFrame or pandas graph API
    nodes_df = nodes_df.cache()

    for _ in range(MAX_ITER):
        sub_df = (
            nodes_df.join(
                edges_df.select(F.col("src").alias("base"), F.col("dst").alias("new_base")), 
                on="base"
            )
            .groupBy("id") 
            .agg(F.min("new_base").alias("new_base"))
        )

        nodes_upd_df = (
                nodes_df.join(sub_df, on="id", how="left")
                .withColumn(
                    "base",
                    F.when(F.col("new_base") < F.col("base"), F.col("new_base"))
                    .otherwise(F.col("base")),
                )
                .drop("new_base")
            )

        upd_count = (
            nodes_upd_df.alias("u")
            .join(nodes_df.alias("o"), "id")
            .where(F.col("u.base") != F.col("o.base"))
            .count()
        )

        nodes_df = nodes_upd_df

        if upd_count == 0: 
            break


    addr_counts_df = (
        spark_session.table(SRC_ADDR).alias("a")
            .join(nodes_df.alias("n"), F.col("a.user_profile_system_number") == F.col("n.id"), "right")
            .filter(F.col("a.record_logical_delete_indicator") == False)
            .groupBy("n.id")
            .count().alias("addr_count")
    )

    pref_counts_df = (
        spark_session.table(SRC_PREF).alias("p")
            .join(nodes_df.alias("n"), F.col("p.user_profile_system_number") == F.col("n.id"), "right")
            .filter(F.col("p.user_preference_selection_indicator") == True)
            .groupBy("n.id")
            .count().alias("pref_count")
    )

    field_count_df = (
        spark_session.table(SRC_PROF).alias("p")
            .join(nodes_df.alias("n"), F.col("p.user_profile_system_number") == F.col("n.id"))
            .select(
                "n.id", 
                F.expr("""
                    CASE WHEN NULLIF(TRIM(user_profile_name), '')       IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN NULLIF(TRIM(electronic_mail_address), '') IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN NULLIF(TRIM(telephone_number), '')        IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN NULLIF(TRIM(user_first_name), '')         IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN NULLIF(TRIM(user_last_name), '')          IS NOT NULL THEN 1 ELSE 0 END       
                """).alias("field_populate_count")
            )
    )

    rownumber_wd = (
        Window.partitionBy("master_contact_identifier_text")
            .orderBy("field_populate_count", "pref_count", "addr_count", "source_system_update_timestamp")
    )

    nodes_with_data_df = (
        nodes_df
        .join(addr_counts_df, on="id")
        .join(pref_counts_df, on="id")
        .join(field_count_df, on="id")
        .select(
            F.col("id").alias("user_profile_system_number"), 
            F.expr("md5(cast(base as string))").alias("k"), 
            F.col("pref_count.count").alias("pref_count"), 
            F.col("addr_count.count").alias("addr_count"),
            F.col("source_system_update_timestamp"),
            F.col("user_profile_delete_indicator"),
            F.col("field_populate_count"),
        )
        .withColumn(
            "master_contact_identifier_text", 
            F.expr("""
                CONCAT_WS('-',
                    SUBSTRING(k, 1, 8),
                    SUBSTRING(k, 9, 4),
                    SUBSTRING(k, 13, 4),
                    SUBSTRING(k, 17, 4),
                    SUBSTRING(k, 21, 12)
                )
            """)
        )
        .withColumn("rn", F.row_number().over(rownumber_wd))
        .withColumn("primary_user_profile_indicator", F.col("rn") == 1)
        .select(
            "user_profile_system_number",
            "master_contact_identifier_text",
            "user_profile_delete_indicator",
            "primary_user_profile_indicator"
        )
    )

    nodes_with_data_df.createOrReplaceTempView("profile_master_updates")

    spark_session.sql(f"""
        MERGE INTO {TGT_TABLE} t
        USING profile_master_updates as s
        ON t.user_profile_system_number = s.user_profile_system_number
        WHEN MATCHED and s.user_profile_delete_indicator = FALSE AND (
            t.master_contact_identifier_text <> s.master_contact_identifier_text OR 
            t.primary_user_profile_indicator <> s.primary_user_profile_indicator
        ) THEN UPDATE SET
            t.master_contact_identifier_text = s.master_contact_identifier_text,
            t.primary_user_profile_indicator = s.primary_user_profile_indicator
        WHEN MATCHED AND s.user_profile_delete_indicator = TRUE THEN DELETE
        WHEN NOT MATCHED THEN INSERT 
            (master_contact_identifier_text, user_profile_system_number, primary_user_profile_indicator)
            VALUES (s.master_contact_identifier_text, s.user_profile_system_number, s.primary_user_profile_indicator)
    """)

user_profile_updates = (
    spark.readStream
    .format("delta")
    # .option("startingVersion", 0)
    .table(SRC_PROF)    
)

query = (
    user_profile_updates
    .writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHK_PATH)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()
