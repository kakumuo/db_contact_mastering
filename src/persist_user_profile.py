from pyspark.sql import functions as F
import usaddress
import pandas as pd
from utils.utils import get_config
from utils import schemas as s

# Get configuration from utils.py
config = get_config()
target_schema = config.get("target_schema")
target_checkpoint_volume_path = config.get("target_checkpoint_volume_path")

user_profile_table_name = f"{target_schema}.slv_user_profile"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {user_profile_table_name} (
        {s.user_profile_schema}
    );
""")

def process_batch_user_profile(df, batch_id):
    target_table_df = spark.table(user_profile_table_name) 
    fields = {
            "user_profile_name": F.col("src.user_profile_name"),
            "electronic_mail_address": F.col("src.electronic_mail_address"),
            "telephone_number": F.col("src.telephone_number"),
            "user_first_name": F.col("src.user_first_name"),
            "user_last_name": F.col("src.user_last_name"),
            "preferred_communication_type_code": F.col("src.preferred_communication_type_code"),
            "source_system_update_timestamp": F.col("src.source_system_update_timestamp"),
            "user_profile_delete_indicator": F.col("src.user_profile_delete_indicator"),
            "user_profile_delete_date": F.col("src.user_profile_delete_date"),
        }

    (df.alias("src").mergeInto(user_profile_table_name, F.expr(f"src.user_profile_name = {user_profile_table_name}.user_profile_name")) 
        .whenMatched().update(fields) 
        .whenNotMatched().insert(fields)
        .merge())

def persist_user_profile_updates(): 
    ref_comm_df = spark.read.table(f"{target_schema}.ref_communication_type")

    user_profile_df = (spark.readStream
        .format("delta")
        .option("startingVersion", 0)
        .table(f"{target_schema}.raw_user") 
        .join(F.broadcast(ref_comm_df).alias("ref_comm")
            , F.col("preferred_contact_type") == F.col("communication_type_text")) 
        .select(
            F.col("username").alias("user_profile_name"), 
            F.col("email").alias("electronic_mail_address"), 
            F.regexp_replace("phone", r"[()\-\s]+", "").alias("telephone_number"), 
            F.col("first_name").alias("user_first_name"), 
            F.col("last_name").alias("user_last_name"), 
            F.col("ref_comm.communication_type_code").alias("preferred_communication_type_code"), 
            F.col("updated_at").alias("source_system_update_timestamp"), 
            F.lit(False).alias("user_profile_delete_indicator"), 
            F.lit(None).alias("user_profile_delete_date")
        ))
    
    query = user_profile_df.writeStream \
        .format("delta") \
        .foreachBatch(process_batch_user_profile) \
        .option("checkpointLocation", f"{target_checkpoint_volume_path}/user_profile_cp") \
        .trigger(availableNow=True) \
        .start()

    query.awaitTermination()

persist_user_profile_updates()


target_table_name = f"{target_schema}.slv_user_preference"
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {target_table_name} (
        {s.user_preference_schema}
    );
""")

def process_batch_user_preference(df, batch_id):
    target_table_df = spark.table(target_table_name) 
    fields = {
            "user_profile_system_number": F.col("src.user_profile_system_number"),
            "source_system_update_timestamp": F.col("src.source_system_update_timestamp"),
            "user_preference_type_code": F.col("src.user_preference_type_code"),
            "user_preference_selection_indicator": F.col("src.user_preference_selection_indicator")
        }

    (df.alias("src").mergeInto(target_table_name, F.expr(f"src.user_profile_system_number = {target_table_name}.user_profile_system_number AND src.user_preference_type_code = {target_table_name}.user_preference_type_code")) 
        .whenMatched().update(fields) 
        .whenNotMatched().insert(fields)
        .merge())

def persist_user_preference(): 
    user_profile_df = spark.read.table(f"{target_schema}.slv_user_profile").alias("usr")
    df = (spark.readStream
        .format("delta")
        .option("startingVersion", 0)
        .table(f"{target_schema}.raw_user") 
        .join(user_profile_df, F.col("usr.user_profile_name") == F.col("username"))
        .select(
            F.col("usr.user_profile_system_number"),
            F.col("source_system_update_timestamp"), 
            F.explode(F.from_json(F.to_json("preferences"), "map<string, boolean>")).alias("key", "value")
        )            
        .select(
            F.col("user_profile_system_number"), 
            F.col("source_system_update_timestamp"), 
            F.when(F.col("key") == F.lit("new_product"), F.lit("NP"))
                .when(F.col("key") == F.lit("educational"), F.lit("ED"))
                .when(F.col("key") == F.lit("discounts"), F.lit("DC"))
                .when(F.col("key") == F.lit("news"), F.lit("NS"))
                .when(F.col("key") == F.lit("insights"), F.lit("IS"))
            .alias("user_preference_type_code"), 
            F.col("value").alias("user_preference_selection_indicator")
        ))
    
    query = df.writeStream \
        .format("delta") \
        .foreachBatch(process_batch_user_preference) \
        .option("checkpointLocation", f"{target_checkpoint_volume_path}/user_preference_cp") \
        .trigger(availableNow=True) \
        .start()

    query.awaitTermination()


persist_user_preference()


target_table_name = f"{target_schema}.slv_user_address"
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {target_table_name} (
        {s.user_address_schema}
    );
""")

def process_batch_user_address(df, batch_id):
    target_table_df = spark.table(target_table_name) 
    fields = {
        "user_profile_system_number": F.col("src.user_profile_system_number"),
        "address_type_code": F.col("src.address_type_code"),
        "street_number": F.col("src.street_number"),
        "street_predirectional": F.col("src.street_predirectional"),
        "street_name": F.col("src.street_name"),
        "street_post_type": F.col("src.street_post_type"),
        "street_postdirectional": F.col("src.street_postdirectional"),
        "unit_type": F.col("src.unit_type"),
        "unit_identifier": F.col("src.unit_identifier"),
        "post_office_box": F.col("src.post_office_box"),
        "building_name": F.col("src.building_name"),
        "place_name": F.col("src.place_name"),
        "political_division_code": F.col("src.political_division_code"),
        "country_code": F.col("src.country_code"),
        "postal_code": F.col("src.postal_code"),
        "record_logical_delete_indicator": F.col("src.record_logical_delete_indicator"),
        "source_system_update_timestamp": F.col("src.source_system_update_timestamp")
    }

    (df.alias("src").mergeInto(target_table_name, F.expr(f"src.user_profile_system_number = {target_table_name}.user_profile_system_number AND src.address_type_code = {target_table_name}.address_type_code")) 
        .whenMatched().update(fields) 
        .whenNotMatched().insert(fields)
        # .whenNotMatchedBySource().update({
        #     "record_logical_delete_indicator": F.lit("True"),
        #     "source_system_update_timestamp": F.lit(datetime.datetime.now()),
        # })
        .merge())

@F.pandas_udf(s.parsed_address_schema)
def parse_address(addresses: pd.Series) -> pd.DataFrame: 
    res = []

    for addr_str in addresses: 
        tagged = {}
        
        addr = str(addr_str).replace("#APT", "APT").replace("#apt", "apt")
        try: 
            tagged, _ = usaddress.tag(addr)
        except: 
            pass

        res.append({
            "street_number": tagged.get("AddressNumber"),
            "street_predirectional": tagged.get("StreetNamePreDirectional"),
            "street_name": tagged.get("StreetName"),
            "street_post_type": tagged.get("StreetNamePostType"),
            "street_postdirectional": tagged.get("StreetNamePostDirectional"),
            "unit_type": tagged.get("OccupancyType"),
            "unit_identifier": tagged.get("OccupancyIdentifier"),
            "post_office_box": tagged.get("USPSBoxID"),
            "building_name": tagged.get("BuildingName"),
        })
        
    return pd.DataFrame(res)

def persist_user_address(): 
    user_profile_df = spark.read.table(f"{target_schema}.slv_user_profile").alias("usr")
    ref_address_type_df = spark.read.table(f"{target_schema}.ref_user_address_type").alias("adt")
    ref_poldiv_df = spark.read.table(f"{target_schema}.ref_political_division").alias("pldv")
    ref_country_df = spark.read.table(f"{target_schema}.ref_country").alias("cty")
    
    df = (spark.readStream
        .format("delta")
        .option("startingVersion", 0)
        .table(f"{target_schema}.raw_user") 
        .join(user_profile_df, F.col("usr.user_profile_name") == F.col("username"))
        .select(
            F.col("usr.user_profile_system_number"),
            F.col("source_system_update_timestamp"), 
            F.explode(F.col("address")).alias("address")
        )
        .select("*", "address.*")
        .withColumn(
            "joined", 
            F.array_join(F.array_compact(
                F.array("address_line_1", "address_line_2", "city", "state", "zip", "country")
            ), ", ")
        )
        .withColumn("parsed", parse_address(F.col("joined")))
        .join(F.broadcast(ref_address_type_df), F.col("adt.user_address_type_text") == F.col("address_type"))
        .join(F.broadcast(ref_poldiv_df), F.col("pldv.political_division_text") == F.col("state"))
        .join(F.broadcast(ref_country_df), F.col("cty.country_text") == F.col("country"))
        .select(
            F.col("user_profile_system_number"), 
            F.col("source_system_update_timestamp"), 
            F.col("adt.user_address_type_code").alias("address_type_code"), 
            F.col("parsed.*"), 
            F.col("city").alias("place_name"),
            F.col("zip").alias("postal_code"), 
            F.col("pldv.political_division_code").alias("political_division_code"), 
            F.col("cty.country_code").alias("country_code"), 
            # F.lit(None).alias("latitude_number"), 
            # F.lit(None).alias("longitude_number"), 
            F.lit(False).alias("record_logical_delete_indicator")
        ))
    
    query = df.writeStream \
        .format("delta") \
        .foreachBatch(process_batch_user_address) \
        .option("checkpointLocation", f"{target_checkpoint_volume_path}/user_address_cp") \
        .trigger(availableNow=True) \
        .start()

    query.awaitTermination()

persist_user_address()