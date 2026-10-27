from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType


# === RAW FILES ==== 
user_raw_file_schema = """
username STRING,
email STRING,  
phone STRING, 
first_name STRING, 
last_name STRING, 
preferred_contact_type STRING, 
preferences STRUCT<new_product:BOOLEAN, educational:BOOLEAN, discounts:BOOLEAN, news:BOOLEAN, insights:BOOLEAN>, 
address ARRAY<STRUCT<address_line_1:STRING, address_line_2:STRING, address_type:STRING, city:STRING, state:STRING, zip:STRING, country:STRING>>, 
updated_at TIMESTAMP
"""


delete_request_raw_file_schema = """
username STRING, 
reason STRING, 
requested_at TIMESTAMP
"""


# === BRONZE === 
user_raw_schema = """
id BIGINT GENERATED ALWAYS AS IDENTITY, 
username STRING,
email STRING,  
phone STRING, 
first_name STRING, 
last_name STRING, 
preferred_contact_type STRING, 
preferences STRUCT<new_product:BOOLEAN, educational:BOOLEAN, discounts:BOOLEAN, news:BOOLEAN, insights:BOOLEAN>, 
address ARRAY<STRUCT<address_line_1:STRING, address_line_2:STRING, address_type:STRING, city:STRING, state:STRING, zip:STRING, country:STRING>>, 
updated_at TIMESTAMP
"""

delete_request_raw_schema = """
id BIGINT GENERATED ALWAYS AS IDENTITY, 
username STRING, 
reason STRING, 
requested_at TIMESTAMP
"""


# === SILVER === 
user_profile_schema = """
user_profile_system_number BIGINT GENERATED ALWAYS AS IDENTITY ,  
user_profile_name STRING, 
electronic_mail_address STRING, 
telephone_number STRING, 
user_first_name STRING, 
user_last_name STRING, 
preferred_communication_type_code STRING, 
source_system_update_timestamp TIMESTAMP, 
user_profile_delete_indicator BOOLEAN, 
user_profile_delete_date TIMESTAMP, 

CONSTRAINT user_profile_pk PRIMARY KEY (user_profile_system_number),
FOREIGN KEY (preferred_communication_type_code) REFERENCES ref_communication_type(communication_type_code)
"""

parsed_address_schema = """
street_number STRING, 
street_predirectional STRING, 
street_name STRING, 
street_post_type STRING, 
street_postdirectional STRING, 
unit_type STRING, 
unit_identifier STRING, 
post_office_box STRING, 
building_name STRING
"""
user_address_schema = """
user_address_system_number BIGINT GENERATED ALWAYS AS IDENTITY ,  
user_profile_system_number BIGINT NOT NULL, 
address_type_code STRING NOT NULL, 
street_number STRING, 
street_predirectional STRING,
street_name STRING,
street_post_type STRING,
street_postdirectional STRING,
unit_type STRING,
unit_identifier STRING,
post_office_box STRING,
building_name STRING, 
place_name STRING, 
political_division_code STRING, 
country_code STRING, 
postal_code STRING, 
record_logical_delete_indicator BOOLEAN, 
source_system_update_timestamp TIMESTAMP,
 
CONSTRAINT user_address_pk PRIMARY KEY (user_address_system_number),
CONSTRAINT uq_user_profile_address_type UNIQUE (user_profile_system_number, address_type_code),
FOREIGN KEY (user_profile_system_number) REFERENCES slv_user_profile(user_profile_system_number),
FOREIGN KEY (address_type_code) REFERENCES ref_user_address_type(user_address_type_code),
FOREIGN KEY (country_code) REFERENCES ref_country(country_code),
FOREIGN KEY (political_division_code) REFERENCES ref_political_division(political_division_code)
"""

user_preference_schema = """
user_preference_system_number BIGINT GENERATED ALWAYS AS IDENTITY , 
user_profile_system_number BIGINT, 
user_preference_type_code STRING, 
user_preference_selection_indicator BOOLEAN, 
source_system_update_timestamp TIMESTAMP, 

CONSTRAINT user_pref_pk PRIMARY KEY (user_preference_system_number),
CONSTRAINT user_profile_user_pref_type_uq UNIQUE (user_profile_system_number, user_preference_type_code),
FOREIGN KEY (user_profile_system_number) REFERENCES slv_user_profile(user_profile_system_number),
FOREIGN KEY (user_preference_type_code) REFERENCES ref_user_preference_type(user_preference_type_code)
"""

user_deletion_reason_schema = """
user_deletion_reason_system_number BIGINT GENERATED ALWAYS AS IDENTITY , 
user_profile_system_number BIGINT, 
user_deletion_reason_type_code STRING, 
user_deletion_timestamp TIMESTAMP,

CONSTRAINT user_del_pk PRIMARY KEY (user_deletion_reason_system_number),
CONSTRAINT user_profile_user_del_uq UNIQUE (user_profile_system_number, user_deletion_reason_type_code),
FOREIGN KEY (user_profile_system_number) REFERENCES slv_user_profile(user_profile_system_number),
FOREIGN KEY (user_deletion_reason_type_code) REFERENCES ref_deletion_reason_type(deletion_reason_type_code)
"""


# === MASTER === 

mst_contact_link_schema = """
master_contact_identifier_text STRING, 
user_profile_system_number BIGINT, 
primary_user_profile_indicator BOOLEAN,

FOREIGN KEY(user_profile_system_number) REFERENCES slv_user_profile(user_profile_system_number)
"""

mst_address_link_schema = """
master_address_identifier_text STRING, 
user_address_system_number BIGINT, 
primary_user_address_indicator BOOLEAN, 

FOREIGN KEY(user_address_system_number) REFERENCES slv_user_address(user_address_system_number)
"""


# === REFEERENCE === 

ref_communication_type_schema = """
communication_type_code STRING PRIMARY KEY, 
communication_type_text STRING
"""

ref_user_address_type_schema = """
user_address_type_code STRING PRIMARY KEY, 
user_address_type_text STRING
"""

ref_user_preference_type_schema = """
user_preference_type_code STRING PRIMARY KEY, 
user_preference_type_text STRING
"""

ref_country_schema = """
country_code STRING PRIMARY KEY, 
country_text STRING
"""

ref_political_division_schema = """
political_division_code STRING PRIMARY KEY, 
political_division_text STRING
"""

ref_deletion_reason_type_schema = """
deletion_reason_type_code STRING PRIMARY KEY,
deletion_reason_text STRING
"""