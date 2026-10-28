
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

MAX_LABEL_ITERATIONS = 100  # safety cap for the label-propagation loop

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

    spark_session.sql(f"""
        WITH changed AS (
            SELECT user_profile_system_number
            FROM user_profile_updates_mb
        ),
        affected_masters AS (
            SELECT DISTINCT master_contact_identifier_text
            FROM mst_contact
            WHERE user_profile_system_number IN (SELECT user_profile_system_number FROM changed)
        ),
        affected_ids AS (
            SELECT user_profile_system_number FROM changed
            UNION
            SELECT m.user_profile_system_number
            FROM mst_contact m
            JOIN affected_masters USING (master_contact_identifier_text)
        )
        SELECT
            p.user_profile_system_number,
            LOWER(TRIM(p.electronic_mail_address)) AS email_key,
            LOWER(TRIM(p.telephone_number))        AS phone_key,
            p.source_system_update_timestamp,
            p.user_profile_delete_indicator,
            (
                CASE WHEN NULLIF(TRIM(user_profile_name), '')       IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN NULLIF(TRIM(electronic_mail_address), '') IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN NULLIF(TRIM(telephone_number), '')        IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN NULLIF(TRIM(user_first_name), '')         IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN NULLIF(TRIM(user_last_name), '')          IS NOT NULL THEN 1 ELSE 0 END
            ) AS field_populate_count
        FROM slv_user_profile p
        JOIN affected_ids USING (user_profile_system_number)
    """).createOrReplaceTempView("tmp_profile_updates")

    
    spark_session.sql("""
        WITH addr_counts AS (
            SELECT user_profile_system_number, COUNT(1) AS ct
            FROM slv_user_address
            WHERE user_profile_system_number IN (SELECT user_profile_system_number FROM profile_updates)
            AND record_logical_delete_indicator = FALSE
            GROUP BY user_profile_system_number
        ),
        pref_counts AS (
            SELECT user_profile_system_number, COUNT(1) AS ct
            FROM slv_user_preference
            WHERE user_profile_system_number IN (SELECT user_profile_system_number FROM profile_updates)
            AND user_preference_selection_indicator = TRUE
            GROUP BY user_profile_system_number
        )
        SELECT
            prf.*,
            COALESCE(adr.ct, 0)  AS address_count,
            COALESCE(pref.ct, 0) AS pref_count
        FROM profile_updates prf
        LEFT JOIN addr_counts adr USING (user_profile_system_number)
        LEFT JOIN pref_counts pref USING (user_profile_system_number)
    """).createOrReplaceTempView("tmp_profile_updates_counts")
    
    
    spark_session.sql("""
        SELECT
            user_profile_system_number AS id,
            user_profile_system_number AS base
        FROM profile_updates a
    """).createOrReplaceTempView("tmp_labels")

    spark_session.sql("""
        WITH CTE AS (
            SELECT
                user_profile_system_number AS id,
                MIN(user_profile_system_number) OVER (PARTITION BY email_key) AS root
            FROM profile_updates a
            WHERE email_key IS NOT NULL
              AND user_profile_delete_indicator = FALSE
            UNION
            SELECT
                user_profile_system_number AS id,
                MIN(user_profile_system_number) OVER (PARTITION BY phone_key) AS root
            FROM profile_updates a
            WHERE phone_key IS NOT NULL
              AND user_profile_delete_indicator = FALSE
        )
        SELECT id AS u, root AS v FROM CTE WHERE id != root
        UNION
        SELECT root, id FROM CTE WHERE id != root
    """).createOrReplaceTempView("tmp_edges")

    for _ in range(MAX_LABEL_ITERATIONS):
        # Rows that would be updated: neighbour's min base is lower than own base
        resultdf = spark_session.sql("""
        MERGE INTO tmp_labels l
        USING (
            SELECT e.u AS id, min(l2.base) AS new_base
            FROM tmp_edges e
            JOIN tmp_labels l2 ON l2.id = e.v
            GROUP BY e.u
        ) m
        ON l.id = m.id
        WHEN MATCHED AND m.new_base < l.base THEN 
        UPDATE SET l.base = m.new_base;
        """)

        metrics = resultdf.collect()[0]
        if metrics["num_affected_rows"] == 0: 
            break

    spark_session.sql("""
        WITH master_key AS (
            SELECT id, MD5(CAST(base AS STRING)) AS k FROM tmp_labels
        )
        SELECT
            a.*,
            CONCAT_WS('-',
                SUBSTRING(k, 1, 8),
                SUBSTRING(k, 9, 4),
                SUBSTRING(k, 13, 4),
                SUBSTRING(k, 17, 4),
                SUBSTRING(k, 21, 12)
            ) AS master_key,
            ROW_NUMBER() OVER (
                PARTITION BY k
                ORDER BY field_populate_count DESC,
                         address_count DESC,
                         pref_count DESC,
                         source_system_update_timestamp
            ) AS primary_record_rn
        FROM profile_updates a
        INNER JOIN master_key b
            ON a.user_profile_system_number = b.id
    """).createOrReplaceTempView("profile_master_updates")

    spark_session.sql(f"""
        MERGE INTO {TGT_TABLE} t
        USING (
            SELECT
                user_profile_system_number, 
                master_key as master_contact_identifier_text, 
                user_profile_delete_indicator, 
                case when s.primary_record_rn = 1 then TRUE else FALSE end as primary_user_profile_indicator
            FROM profile_master_updates
        ) s
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
    spark.table("user_profile_updates")
    .writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHK_PATH)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()



"""
CREATE OR REPLACE TEMPORARY TABLE profile_updates AS
WITH changed AS (
    SELECT user_profile_system_number
    FROM slv_user_profile
    WHERE source_system_update_timestamp < TIMESTAMP_NTZ '2026-10-27 03:56:47.591+00'
),
affected_masters AS (
    SELECT DISTINCT master_contact_identifier_text
    FROM mst_contact
    WHERE user_profile_system_number IN (SELECT user_profile_system_number FROM changed)
),
affected_ids AS (
    SELECT user_profile_system_number FROM changed
    UNION
    SELECT m.user_profile_system_number
    FROM mst_contact m
    JOIN affected_masters USING (master_contact_identifier_text)
)
SELECT
    p.user_profile_system_number,
    LOWER(TRIM(p.electronic_mail_address)) AS email_key,
    LOWER(TRIM(p.telephone_number))        AS phone_key,
    p.source_system_update_timestamp,
    p.user_profile_delete_indicator, 
    (
        case when nullif(trim(user_profile_name), '')       is not null then 1 else 0 end + 
        case when nullif(trim(electronic_mail_address), '') is not null then 1 else 0 end + 
        case when nullif(trim(telephone_number), '')        is not null then 1 else 0 end + 
        case when nullif(trim(user_first_name), '')         is not null then 1 else 0 end + 
        case when nullif(trim(user_last_name), '')          is not null then 1 else 0 end
    ) as field_populate_count
FROM slv_user_profile p
JOIN affected_ids USING (user_profile_system_number)
;


create or replace temporary table profile_updates as
with addr_counts as (
    select 
        user_profile_system_number, 
        count(1) as ct
    from slv_user_address
    where user_profile_system_number in (select user_profile_system_number from profile_updates)
    and record_logical_delete_indicator = false
    group by user_profile_system_number
), pref_counts as (
    select 
        user_profile_system_number, 
        count(1) as ct
    from slv_user_preference
    where user_profile_system_number in (select user_profile_system_number from profile_updates)
    and user_preference_selection_indicator = true
    group by user_profile_system_number
)  
select 
    prf.*,
    coalesce(adr.ct, 0) as address_count,
    coalesce(pref.ct, 0) as pref_count 
FROM profile_updates prf
LEFT JOIN addr_counts adr USING (user_profile_system_number)
LEFT JOIN pref_counts pref USING (user_profile_system_number)
;


CREATE OR REPLACE TEMPORARY TABLE tmp_labels AS
SELECT
    user_profile_system_number as id,
    user_profile_system_number as base 
FROM profile_updates a;

CREATE OR REPLACE TEMPORARY TABLE tmp_edges AS
WITH CTE AS (
    SELECT
        user_profile_system_number as id,
        min(user_profile_system_number) over (partition by email_key) as root 
    FROM profile_updates a
    WHERE email_key IS NOT NULL
    AND user_profile_delete_indicator = FALSE
    UNION
    SELECT
        user_profile_system_number as id,
        min(user_profile_system_number) over (partition by phone_key) as root 
    FROM profile_updates a
    WHERE phone_key IS NOT NULL
    AND user_profile_delete_indicator = FALSE
)
SELECT id as u, root as v FROM CTE WHERE id != root
UNION
SELECT root, id FROM CTE WHERE id != root 



-- execute in loop while num affected rows > 0
MERGE INTO tmp_labels l
USING (
    SELECT e.u AS id, min(l2.base) AS new_base
    FROM tmp_edges e
    JOIN tmp_labels l2 ON l2.id = e.v
    GROUP BY e.u
) m
ON l.id = m.id
WHEN MATCHED AND m.new_base < l.base THEN 
UPDATE SET l.base = m.new_base;


-- merge into target table
;WITH master_key AS (
    select id, md5(cast(base as string)) k from tmp_labels
)
SELECT 
    a.*, 
    CONCAT_WS('-', SUBSTRING(k, 1,8), SUBSTRING(k, 9,4), SUBSTRING(k, 13,4), SUBSTRING(k, 17,4), SUBSTRING(k, 21,12)) master_key, 
    row_number() over (partition by k order by field_populate_count desc, address_count desc, pref_count desc, source_system_update_timestamp) primary_record_rn
FROM profile_updates a
INNER JOIN master_key b
ON a.user_profile_system_number = b.id
"""
