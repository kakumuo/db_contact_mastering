from pyspark.sql import functions as F
from utils.utils import get_config
from utils import schemas as s

config = get_config()
target_schema = config.target_schema

spark.sql(f"USE {target_schema}")

def init_communication_type(): 
    table_name = "ref_communication_type"
    schema = s.ref_communication_type_schema
    key_col = "communication_type_code"

    data = [
        ("EM", "Email"), ("PH", "Phone")
    ]
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {s.ref_communication_type_schema}
        );
    """)

    tgt_df = spark.table(table_name)

    spark.createDataFrame(data, schema).alias("src") \
        .mergeInto(table_name, F.col(f"src.{key_col}") == F.col(f"{table_name}.{key_col}")) \
        .whenMatched().updateAll() \
        .whenNotMatched().insertAll() \
        .merge()

def init_address_type(): 
    table_name = "ref_user_address_type"
    schema = s.ref_user_address_type_schema
    key_col = "user_address_type_code"

    data = [
        ("SH", "Shipping"), 
        ("BL", "Billing"),
        ("ML", "Mailing"),
        ("PH", "Physical"),
    ]

        
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {schema}
        );
    """)

    tgt_df = spark.table(table_name)

    spark.createDataFrame(data, schema).alias("src") \
        .mergeInto(table_name, F.col(f"src.{key_col}") == F.col(f"{table_name}.{key_col}")) \
        .whenMatched().updateAll() \
        .whenNotMatched().insertAll() \
        .merge()


def init_user_preference_type(): 
    table_name = "ref_user_preference_type"
    schema = s.ref_user_preference_type_schema
    key_col = "user_preference_type_code"
    data = [
        ("NP", "New Product"), 
        ("ED", "Educational"),
        ("DC", "Discounts"),
        ("IS", "Insights"),
        ("NS", "News"),
    ]

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {schema}
        );
    """)

    tgt_df = spark.table(table_name)

    spark.createDataFrame(data, schema).alias("src") \
        .mergeInto(table_name, F.col(f"src.{key_col}") == F.col(f"{table_name}.{key_col}")) \
        .whenMatched().updateAll() \
        .whenNotMatched().insertAll() \
        .merge()

def init_country(): 
    table_name = "ref_country"
    schema = s.ref_country_schema
    key_col = "country_code"
    data = [
        ("US", "United States") 
    ]

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {schema}
        );
    """)

    tgt_df = spark.table(table_name)

    spark.createDataFrame(data, schema).alias("src") \
        .mergeInto(table_name, F.col(f"src.{key_col}") == F.col(f"{table_name}.{key_col}")) \
        .whenMatched().updateAll() \
        .whenNotMatched().insertAll() \
        .merge()

def init_political_division(): 
    table_name = "ref_political_division"
    schema = s.ref_political_division_schema
    key_col = "political_division_code"

    data = [
        ("AL","Alabama"),("AK","Alaska"),("AS","American Samoa"),("AZ","Arizona"),("AR","Arkansas"),("CA","California"),("CO","Colorado"),("CT","Connecticut"),("DE","Delaware"),("DC","Dist. of Columbia"),("FL","Florida"),("GA","Georgia"),("GU","Guam"),("HI","Hawaii"),("ID","Idaho"),("IL","Illinois"),("IN","Indiana"),("IA","Iowa"),("KS","Kansas"),("KY","Kentucky"),("LA","Louisiana"),("ME","Maine"),("MD","Maryland"),("MH","Marshall Islands"),("MA","Massachusetts"),("MI","Michigan"),("FM","Micronesia"),("MN","Minnesota"),("MS","Mississippi"),("MO","Missouri"),("MT","Montana"),("NE","Nebraska"),("NV","Nevada"),("NH","New Hampshire"),("NJ","New Jersey"),("NM","New Mexico"),("NY","New York"),("NC","North Carolina"),("ND","North Dakota"),("MP","Northern Marianas"),("OH","Ohio"),("OK","Oklahoma"),("OR","Oregon"),("PW","Palau"),("PA","Pennsylvania"),("PR","Puerto Rico"),("RI","Rhode Island"),("SC","South Carolina"),("SD","South Dakota"),("TN","Tennessee"),("TX","Texas"),("UT","Utah"),("VT","Vermont"),("VA","Virginia"),("VI","Virgin Islands"),("WA","Washington"),("WV","West Virginia"),("WI","Wisconsin"),("WY","Wyoming")
    ]

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {schema}
        );
    """)

    tgt_df = spark.table(table_name)

    spark.createDataFrame(data, schema).alias("src") \
        .mergeInto(table_name, F.col(f"src.{key_col}") == F.col(f"{table_name}.{key_col}")) \
        .whenMatched().updateAll() \
        .whenNotMatched().insertAll() \
        .merge()

def init_deletion_reason_type(): 
    table_name = "ref_deletion_reason_type"
    schema = s.ref_deletion_reason_type_schema
    key_col = "deletion_reason_type_code"

    data = [
        ("NEEDS", "Needs not met"),
        ("CUSTOMER_SUPPORT", "Poor customer support"),
        ("PRIVACY_SECURITY", "Privacy or security concerns"),
        ("PRICING_VALUE", "Price too high or poor value"),
        ("POLICY_TERMS_CHANGE", "Terms or policies changed"),
        ("COMPETITIVE_ALTERNATIVE", "Better competitor alternative"),
        ("TECHNICAL_ISSUES", "Technical issues or reliability problems"),
        ("ACCOUNT_CONTROL", "Insufficient account control"),
        ("TRUST", "Lost trust in the service"),
        ("UNWANTED_COMMUNICATIONS", "Too many unwanted communications"),
        ("NO_LONGER_NEEDED", "Service no longer needed"),
        ("PERSONAL_CIRCUMSTANCES", "Personal circumstances changed"),
        ("COMPANY_VALUES", "Concerns about company values"),
    ]

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {schema}
        );
    """)

    tgt_df = spark.table(table_name)

    spark.createDataFrame(data, schema).alias("src") \
        .mergeInto(table_name, F.col(f"src.{key_col}") == F.col(f"{table_name}.{key_col}")) \
        .whenMatched().updateAll() \
        .whenNotMatched().insertAll() \
        .merge()

init_communication_type()
init_address_type()
init_user_preference_type()
init_country()
init_political_division()
init_deletion_reason_type()