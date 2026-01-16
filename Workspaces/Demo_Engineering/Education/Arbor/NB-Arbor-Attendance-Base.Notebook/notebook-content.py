# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# #### Set Up

# CELL ********************

# MAGIC %%configure -f
# MAGIC {
# MAGIC     "defaultLakehouse": {
# MAGIC         "name": "LH_Raw"
# MAGIC     }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql.functions import to_timestamp, to_date, col, date_format, lit, current_date, date_sub, hash, concat_ws, when, regexp_replace, substring
from delta.tables import *
from datetime import datetime, timedelta


RawLH = "LH_Raw"
BaseLH = "LH_Base"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Roll Call Attendance

# CELL ********************

# Read data from raw layer
# df = spark.read.format("delta").table(f"{RawLH}.edu_arbor_roll_call_attendance")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("""
    WITH valid_attendance AS (
    SELECT /*+ BROADCAST(y) */
            a.attendance_roll_call_record_unique_id
    FROM LH_Raw.edu_arbor_roll_call_attendance a
    LEFT SEMI JOIN LH_Base.edu_arbor_student_school_enrolments y
        ON  a.student_unique_id = y.student_unique_id
        AND a.date >= y.start_date
        AND (a.date <= y.end_date OR end_date IS NULL)
    )
    SELECT
        a.attendance_roll_call_record_id
      ,a.attendance_roll_call_record_unique_id
      ,a.student_unique_id
      ,a.student_id
      ,a.application_id
      ,a.attendance_roll_call_id
      ,a.attendance_roll_call_unique_id
      ,a.raw_mark
      ,a.mark_code
      ,a.minutes_late
      ,a.period
      ,a.date
      ,a.academic_year_unique_id
      ,a.academic_year_id
      ,a.academic_year_name
      ,a.is_present
      ,a.is_authorized_absent
      ,a.is_unauthorized_absent
      ,a.is_possible_attendance
      ,a.record_changed_in_warehouse_datetime
      ,a.academic_year_code
      ,a.extraction_date_time
      ,CASE WHEN v.attendance_roll_call_record_unique_id IS NULL THEN 0 ELSE 1 END AS valid_mark
    FROM LH_Raw.edu_arbor_roll_call_attendance a
    LEFT JOIN valid_attendance v
    ON a.attendance_roll_call_record_unique_id = v.attendance_roll_call_record_unique_id; """)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_min = spark.sql(f"""
SELECT MIN (a.date) AS `min_date` FROM LH_Raw.edu_arbor_roll_call_attendance AS a
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

min_date = df_min.collect()[0]["min_date"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(min_date)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Merge into base layer.

if spark.catalog.tableExists(f"{BaseLH}.edu_arbor_roll_call_attendance"):
    # Reference Base Table
    base_table = DeltaTable.forName(spark, f"{BaseLH}.edu_arbor_roll_call_attendance")

    # Define the merge condition
    merge_condition = f"raw.attendance_roll_call_record_unique_id = base.attendance_roll_call_record_unique_id"

    # Use merge
    (base_table.alias("base")
        .merge(df.alias("raw"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete(condition = f"base.date >= '{min_date}'")
        .execute())
else:
    # Create new table
    df.write.format("delta").saveAsTable(f"{BaseLH}.edu_arbor_roll_call_attendance")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT COUNT(*) FROM LH_Base.edu_arbor_roll_call_attendance

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
