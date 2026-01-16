# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Setup Meta Data for Arbor Ingest

# MARKDOWN ********************

# ## Imports

# CELL ********************

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import DataFrame
from pyspark.sql.functions import to_timestamp, to_date, col, date_format, lit, current_date, date_sub, hash, concat_ws, when, regexp_replace, substring, explode, concat
from delta.tables import *
from datetime import datetime, timedelta
import sempy.fabric as fabric
import json

from notebookutils import variableLibrary
from pyspark.sql.types import *

RawLH = "LH_Raw"
BaseLH = "LH_Base"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Parametisation

# CELL ********************

# Get the entire variable library by name
vl = variableLibrary.getLibrary("VL_CORE")

# Fetch key environment variables
STORE_WORKSPACE_NAME = vl.getVariable("STORE_WORKSPACE_NAME")
STORE_WORKSPACE_ID = vl.getVariable("STORE_WORKSPACE_ID")
BASE_LH_NAME = vl.getVariable("BASE_LH_NAME")
RAW_LH_NAME = vl.getVariable("RAW_LH_NAME")

# Explicitly name all lakehouse tables

base_meta_table_name = f"{STORE_WORKSPACE_NAME}.{BASE_LH_NAME}.edu.arbor_ingest_metadata"

print(f"Meta table name: {base_meta_table_name}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Create Arbor Ingest Metadata table

# MARKDOWN ********************

# ### Schema

# CELL ********************

schema = StructType([
    StructField("ingest_data_id", IntegerType(), True),
    StructField("source_connection_name", StringType(), True),
    StructField("source_connection_type", StringType(), True),
    StructField("source_database", StringType(), True),
    StructField("source_sql", StringType(), True),
    StructField("destination_workspace_id", StringType(), True),
    StructField("destination_lakehouse", StringType(), True),
    StructField("destination_schema", StringType(), True),
    StructField("destination_table", StringType(), True),
    StructField("mapping", StringType(), True),
    StructField("refresh_frequency", IntegerType(), True),
    StructField("data_classification", StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Intervention Sessions

# MARKDOWN ********************

# #### SQL

# CELL ********************

sql_intervention_sessions = """
    SELECT 
        INTERVENTION_UNIQUE_ID AS "intervention_unique_id", 
        INTERVENTION_ID AS "intervention_id", 
        APPLICATION_ID AS "application_id", 
        INTERVENTION_NAME AS "intervention_name", 
        ACADEMIC_YEAR_UNIQUE_ID AS "academic_year_unique_id", 
        ACADEMIC_YEAR_ID AS "academic_year_id",
        ACADEMIC_YEAR_NAME AS "academic_year_name", 
        TERM_UNIQUE_ID AS "term_unique_id", 
        TERM_ID AS "term_id", 
        CAST(START_DATETIME AS STRING) AS "start_datetime", 
        CAST(START_DATETIME AS DATE) AS start_datetime_date, 
        CAST(END_DATETIME AS STRING) AS "end_datetime", 
        CAST (END_DATETIME AS DATE) AS end_datetime_date, 
        SUBJECT AS "subject", 
        IS_COMPLETED AS "is_completed", 
        LOCATION_UNIQUE_ID AS "location_unique_id", 
        LOCATION_ID AS "location_id", 
        ESTIMATED_DURATION_HOURS AS "estimated_duration_hours", 
        CATEGORY_NAME AS "category_name", 
        CAST(CURRENT_TIMESTAMP() AS string) AS "extraction_date_time" 
    FROM ARBOR_BI_CONNECTOR_PRODUCTION.ARBOR_MIS_ENGLAND_MODELLED.INTERVENTIONS"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Mapping

# CELL ********************

# Might not be needed
mapping_intervention_sessions = """
{
  "mappings": [
    {
      "sink": {
        "name": "intervention_unique_id",
        "physicalType": "string"
      },
      "source": {
        "name": "intervention_unique_id",
        "type": "String"
      }
    },
    {
      "sink": {
        "name": "intervention_id",
        "physicalType": "integer"
      },
      "source": {
        "name": "intervention_id",
        "type": "Decimal",
        "scale": 0,
        "precision": 38
      }
    },
    {
      "sink": {
        "name": "application_id",
        "physicalType": "string"
      },
      "source": {
        "name": "application_id",
        "type": "String"
      }
    },
    {
      "sink": {
        "name": "intervention_name",
        "physicalType": "string"
      },
      "source": {
        "name": "intervention_name",
        "type": "String"
      }
    },
    {
      "sink": {
        "name": "academic_year_unique_id",
        "physicalType": "string"
      },
      "source": {
        "name": "academic_year_unique_id",
        "type": "String"
      }
    },
    {
      "sink": {
        "name": "academic_year_id",
        "physicalType": "integer"
      },
      "source": {
        "name": "academic_year_id",
        "type": "Decimal",
        "scale": 0,
        "precision": 38
      }
    },
    {
      "sink": {
        "name": "academic_year_name",
        "physicalType": "string"
      },
      "source": {
        "name": "academic_year_name",
        "type": "String"
      }
    },
    {
      "sink": {
        "name": "term_unique_id",
        "physicalType": "string"
      },
      "source": {
        "name": "term_unique_id",
        "type": "String"
      }
    },
    {
      "sink": {
        "name": "term_id",
        "physicalType": "integer"
      },
      "source": {
        "name": "term_id",
        "type": "Decimal",
        "source": 0,
        "precision": 38
      }
    },
    {
      "sink": {
        "name": "start_datetime",
        "physicalType": "string"
      },
      "source": {
        "name": "start_datetime",
        "type": "String"
      }
    },
    {
      "sink": {
        "name": "end_datetime",
        "physicalType": "string"
      },
      "source": {
        "name": "end_datetime",
        "type": "String"
      }
    },
    {
      "sink": {
        "name": "subject",
        "physicalType": "integer"
      },
      "source": {
        "name": "subject",
        "precision": 38,
        "scale": 0,
        "type": "Decimal"
      }
    },
    {
      "sink": {
        "name": "is_completed",
        "physicalType": "integer"
      },
      "source": {
        "name": "is_completed",
        "precision": 38,
        "scale": 0,
        "type": "Decimal"
      }
    },
    {
      "sink": {
        "name": "location_unique_id",
        "physicalType": "string"
      },
      "source": {
        "name": "location_unique_id",
        "type": "String"
      }
    },
    {
      "sink": {
        "name": "location_id",
        "physicalType": "integer"
      },
      "source": {
        "name": "location_id",
        "precision": 38,
        "scale": 0,
        "type": "Decimal"
      }
    },
    {
      "sink": {
        "name": "estimated_duration_hours",
        "physicalType": "float"
      },
      "source": {
        "name": "estimated_duration_hours",
        "precision": 38,
        "scale": 6,
        "type": "Decimal"
      }
    },
    {
      "sink": {
        "name": "category_name",
        "physicalType": "string"
      },
      "source": {
        "name": "category_name",
        "type": "String"
      }
    },
    {
      "sink": {
        "name": "extraction_date_time",
        "physicalType": "string"
      },
      "source": {
        "name": "extraction_date_time",
        "type": "DateTimeOffset"
      }
    }
  ],
  "type": "TabularTranslator",
  "typeConversion": true,
  "typeConversionSettings": {
    "allowDataTruncation": false,
    "treatBooleanAsNumber": false
  }
}
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Row

# CELL ********************

row_arbor_intervention_sessions ={
    "ingest_data_id": 1,
    "source_connection_name": "Arbor Snowflake",
    "source_connection_type": "Snowflake",
    "source_database": "ARBOR_BI_CONNECTOR_PRODUCTION",
    "source_sql": sql_intervention_sessions,
    "destination_workspace_id": STORE_WORKSPACE_ID,
    "destination_lakehouse": RAW_LH_NAME,
    "destination_schema": "edu",
    "destination_table": "arbor_interventions",
    "mapping": mapping_intervention_sessions,
    "refresh_frequency": 1,
    "data_classification": "Interventions"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Intervention Session Attendance

# MARKDOWN ********************

# #### SQL

# CELL ********************

sql_intervention_session_attendance = """
    SELECT
        INTERVENTION_ATTENDANCE_RECORD_UNIQUE_ID AS "intervention_attendance_record_unique_id",
        INTERVENTION_ATTENDANCE_RECORD_ID AS "intervention_attendance_record_id",
        APPLICATION_ID AS "application_id",
        STUDENT_UNIQUE_ID AS "student_unique_id",
        STUDENT_ID AS "student_id",
        RAW_MARK AS "raw_mark",
        MARK_CODE AS "mark_code",
        INTERVENTION_UNIQUE_ID AS "intervention_unique_id",
        INTERVENTION_ID AS "intervention_id",
        INTERVENTION_SESSION_UNIQUE_ID AS "intervention_session_unique_id",
        INTERVENTION_SESSION_ID AS "intervention_session_id",
        CAST(START_DATETIME AS STRING) AS "start_datetime",
        CAST(END_DATETIME AS STRING) AS "end_datetime",
        MINUTES_LATE AS "minutes_late",
        IS_PRESENT AS "is_present",
        IS_AUTHORIZED_ABSENT AS "is_authorized_absent",
        IS_UNAUTHORIZED_ABSENT AS "is_unauthorized_absent",
        IS_POSSIBLE_ATTENDANCE AS "is_possible_attendance",
        CAST(CURRENT_TIMESTAMP() AS STRING) AS "extraction_date_time" 
    FROM ARBOR_BI_CONNECTOR_PRODUCTION.ARBOR_MIS_ENGLAND_MODELLED.INTERVENTION_ATTENDANCE
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Mapping

# CELL ********************

mapping_intervention_session_attendance = """
{
  "type": "TabularTranslator",
  "mappings": [
    {
      "source": {
        "name": "intervention_attendance_record_unique_id",
        "type": "String"
      },
      "sink": {
        "name": "intervention_attendance_record_unique_id",
        "physicalType": "string"
      }
    },
    {
      "source": {
        "name": "intervention_attendance_record_id",
        "type": "Decimal",
        "scale": 0,
        "precision": 38
      },
      "sink": {
        "name": "intervention_attendance_record_id",
        "physicalType": "integer"
      }
    },
    {
      "source": {
        "name": "application_id",
        "type": "String"
      },
      "sink": {
        "name": "application_id",
        "physicalType": "string"
      }
    },
    {
      "source": {
        "name": "student_unique_id",
        "type": "String"
      },
      "sink": {
        "name": "student_unique_id",
        "physicalType": "string"
      }
    },
    {
      "source": {
        "name": "student_id",
        "type": "Decimal",
        "scale": 0,
        "precision": 38
      },
      "sink": {
        "name": "student_id",
        "physicalType": "integer"
      }
    },
    {
      "source": {
        "name": "raw_mark",
        "type": "String"
      },
      "sink": {
        "name": "raw_mark",
        "physicalType": "string"
      }
    },
    {
      "source": {
        "name": "mark_code",
        "type": "String"
      },
      "sink": {
        "name": "mark_code",
        "physicalType": "string"
      }
    },
    {
      "source": {
        "name": "intervention_unique_id",
        "type": "String"
      },
      "sink": {
        "name": "intervention_unique_id",
        "physicalType": "string"
      }
    },
    {
      "source": {
        "name": "intervention_id",
        "type": "Decimal",
        "scale": 0,
        "precision": 38
      },
      "sink": {
        "name": "intervention_id",
        "physicalType": "integer"
      }
    },
    {
      "source": {
        "name": "intervention_session_unique_id",
        "type": "String"
      },
      "sink": {
        "name": "intervention_session_unique_id",
        "physicalType": "string"
      }
    },
    {
      "source": {
        "name": "intervention_session_id",
        "type": "Decimal",
        "scale": 0,
        "precision": 38
      },
      "sink": {
        "name": "intervention_session_id",
        "physicalType": "integer"
      }
    },
    {
      "source": {
        "name": "start_datetime",
        "type": "String"
      },
      "sink": {
        "name": "start_datetime",
        "physicalType": "string"
      }
    },
    {
      "source": {
        "name": "end_datetime",
        "type": "String"
      },
      "sink": {
        "name": "end_datetime",
        "physicalType": "string"
      }
    },
    {
      "source": {
        "name": "minutes_late",
        "type": "Decimal",
        "scale": 0,
        "precision": 38
      },
      "sink": {
        "name": "minutes_late",
        "physicalType": "integer"
      }
    },
    {
      "source": {
        "name": "is_present",
        "type": "Boolean"
      },
      "sink": {
        "name": "is_present",
        "physicalType": "boolean"
      }
    },
    {
      "source": {
        "name": "is_authorized_absent",
        "type": "Boolean"
      },
      "sink": {
        "name": "is_authorized_absent",
        "physicalType": "boolean"
      }
    },
    {
      "source": {
        "name": "is_unauthorized_absent",
        "type": "Boolean"
      },
      "sink": {
        "name": "is_unauthorized_absent",
        "physicalType": "boolean"
      }
    },
    {
      "source": {
        "name": "is_possible_attendance",
        "type": "Boolean"
      },
      "sink": {
        "name": "is_possible_attendance",
        "physicalType": "boolean"
      }
    },
    {
      "source": {
        "name": "extraction_date_time",
        "type": "DateTimeOffset"
      },
      "sink": {
        "name": "extraction_date_time",
        "physicalType": "string"
      }
    }
  ],
  "typeConversion": true,
  "typeConversionSettings": {
    "allowDataTruncation": false,
    "treatBooleanAsNumber": false
  }
}
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Row

# CELL ********************

row_arbor_intervention_session_attendance ={
    "ingest_data_id": 2,
    "source_connection_name": "Arbor Snowflake",
    "source_connection_type": "Snowflake",
    "source_database": "ARBOR_BI_CONNECTOR_PRODUCTION",
    "source_sql": sql_intervention_session_attendance,
    "destination_workspace_id": STORE_WORKSPACE_ID,
    "destination_lakehouse": RAW_LH_NAME,
    "destination_schema": "edu",
    "destination_table": "arbor_intervention_attendance",
    "mapping": mapping_intervention_session_attendance,
    "refresh_frequency": 1,
    "data_classification": "Interventions"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Write Data

# CELL ********************

data = [
    row_arbor_intervention_sessions,
    row_arbor_intervention_session_attendance]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.createDataFrame(data, schema=schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(base_meta_table_name)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
