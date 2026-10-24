from pyspark.sql import functions as F
from src.utils.utils import get_config
from src.utils import schemas as s

# Get configuration from utils.py
config = get_config()
target_schema = config.get("target_schema")
target_checkpoint_volume_path = config.get("target_checkpoint_volume_path")

target_table_name = f"{target_schema}.slv_user_deletion_reason"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {target_table_name} (
        {s.user_deletion_reason_schema}
    );
""")

def process_batch_user_delete_reason(df, batch_id):
    target_table_df = spark.table(target_table_name) 
    fields = {
            "user_profile_system_number": F.col("src.user_profile_system_number"),
            "user_deletion_reason_type_code": F.col("src.user_deletion_reason_type_code"),
            "user_deletion_timestamp": F.col("src.user_deletion_timestamp"),
        }

    (
        df.alias("src")
        .mergeInto(target_table_name, 
                   F.expr(f"src.user_profile_system_number = {target_table_name}.user_profile_system_number AND src.user_deletion_reason_type_code = {target_table_name}.user_deletion_reason_type_code")) 
        .whenMatched().update(fields) 
        .whenNotMatched().insert(fields)
        .merge()
    )
    
def process_batch_user_profile_delete(df, batch_id):
    user_profile_table_name = f"{target_schema}.slv_user_profile"

    (
        df.alias("src") 
        .mergeInto(user_profile_table_name,
                    F.expr(f"src.user_profile_system_number = {user_profile_table_name}.user_profile_system_number"))
        .whenMatched().update({
            "user_profile_delete_indicator": F.col("src.user_profile_delete_indicator"),
            "user_profile_delete_date": F.col("src.user_profile_delete_date"),
            "source_system_update_timestamp": F.col("src.source_system_update_timestamp")
        }).merge()
    )
    

def persist_user_deletion_reason_updates():
    user_profile_df = spark.read.table(f"{target_schema}.slv_user_profile").alias("usr")

    delete_reason_data = spark.read.table(f"{target_schema}.ref_deletion_reason_type").collect()
    labels = {row["deletion_reason_type_code"]: row["deletion_reason_text"] for row in delete_reason_data}
    # FIXME: getting error when using ref table -> An error occurred while compiling the AI function: INVALID_ARGUMENT: Must provide at least 1 unique label.

    # labels = ["NEEDS", "CUSTOMER_SUPPORT", "PRIVACY_SECURITY", "PRICING_VALUE", "POLICY_TERMS_CHANGE", "COMPETITIVE_ALTERNATIVE", "TECHNICAL_ISSUES", "ACCOUNT_CONTROL", "TRUST", "UNWANTED_COMMUNICATIONS", "NO_LONGER_NEEDED", "PERSONAL_CIRCUMSTANCES", "COMPANY_VALUES"]

    raw_joined_df = (spark.readStream
            .format("delta")
            .option("startingVersion", 0)
            .table(f"{target_schema}.raw_delete_request").alias("raw")
            .join(user_profile_df.alias("usr"),F.col("usr.user_profile_name") == F.col("raw.username"))
    )
    
    user_delete_reason_df = (raw_joined_df.withColumn(
                "ai_raw_output", 
                F.ai_classify(F.col("raw.reason"), labels, options={"version":"2.1", "multilabel":True})
            )
            .withColumn(
                "ai_labeled", 
                F.explode(F.expr("ai_raw_output:response::ARRAY<STRUCT<value:STRING>>"))
            )
            .select(
                "usr.user_profile_system_number",
                F.col("ai_labeled").getField("value").alias("user_deletion_reason_type_code"),
                F.col("requested_at").alias("user_deletion_timestamp")
            )
        )
    
    user_profile_delete_df = (
        raw_joined_df.select(
            "usr.user_profile_system_number", 
            F.lit(True).alias("user_profile_delete_indicator"),
            F.col("raw.requested_at").alias("user_profile_delete_date"),
            F.col("raw.requested_at").alias("source_system_update_timestamp")
        )
    )
    
    q1 = user_delete_reason_df.writeStream \
        .format("delta") \
        .foreachBatch(process_batch_user_delete_reason) \
        .option("checkpointLocation", f"{target_checkpoint_volume_path}/user_deletion_reason_cp") \
        .trigger(availableNow=True) \
        .start()

    q1.awaitTermination()

    q2 = user_profile_delete_df.writeStream \
        .format("delta") \
        .foreachBatch(process_batch_user_profile_delete) \
        .option("checkpointLocation", f"{target_checkpoint_volume_path}/user_profile_delete_cp") \
        .trigger(availableNow=True) \
        .start()

    q2.awaitTermination()

persist_user_deletion_reason_updates()