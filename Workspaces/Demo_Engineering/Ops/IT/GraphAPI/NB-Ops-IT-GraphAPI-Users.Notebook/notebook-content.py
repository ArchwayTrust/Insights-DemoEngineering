# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     },
# META     "environment": {
# META       "environmentId": "d34e2104-f2da-4c22-b1e9-02a2e924b9d9",
# META       "workspaceId": "4db79e65-7f48-4ebe-a27f-523ccba98303"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Process Data from Graph API Users

# MARKDOWN ********************

# ## Parametised Setup
# - This code block should be standard on all notebooks which write to a lakehouse
# - Read, write and merge should always be done with fully qualified table references rather than paths (see examples below)
# 
# **Consider this a first step for migration to schema enabled workbooks and an improved git strategy.**

# CELL ********************

from notebookutils import variableLibrary

# Get the entire variable library by name
vl = variableLibrary.getLibrary("VL_CORE")

# Fetch key environment variables
STORE_WORKSPACE_NAME = vl.getVariable("STORE_WORKSPACE_NAME")
BASE_LH_NAME = vl.getVariable("BASE_LH_NAME")
RAW_LH_NAME = vl.getVariable("RAW_LH_NAME")
SCHEMA_ENABLED = vl.getVariable("SCHEMA_ENABLED")

# Explicitly name all lakehouse tables for both legacy and schema enabled environments
if SCHEMA_ENABLED:
    raw_user_table_name = f"{STORE_WORKSPACE_NAME}.{RAW_LH_NAME}.it.entra_users"
    base_user_table_name = f"{STORE_WORKSPACE_NAME}.{BASE_LH_NAME}.it.entra_users"
else:
    raw_user_table_name = f"{STORE_WORKSPACE_NAME}.{RAW_LH_NAME}.dbo.entra_users"
    base_user_table_name = f"{STORE_WORKSPACE_NAME}.{BASE_LH_NAME}.dbo.entra_users"

print(f"SCHEMA_ENABLE is set to {SCHEMA_ENABLED} so seting up as follows:")
print(f"Raw User Table: {raw_user_table_name}")
print(f"Base User Table: {base_user_table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Standard Code Import

# CELL ********************

%run NB-Ops-IT-GraphAPI-StandardCode

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run NB-Ops-IT-GraphAPI-Structs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Raw Ingest

# CELL ********************

access_token = get_access_token(tenant_id, client_id, client_secret)
request_headers = {'Authorization': f'Bearer {access_token}'}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Basic User Ingest

# CELL ********************

request_url = f"https://graph.microsoft.com/v1.0/users?$select=id,employeeId,userPrincipalName,mail,companyName,surname,displayName,jobTitle,accountEnabled,userType,onPremisesExtensionAttributes,onPremisesSamAccountName,signInActivity,onPremisesSyncEnabled"
#Could filter using $filter=accountEnabled eq true and userType eq 'Member'&

df_users = get_graph_api(request_url, request_headers, graph_api_user_schema, "value")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_users_clean = df_users.select(
                    col("id").alias("user_id"),
                    col("employeeId").alias("employee_id"),
                    col("userPrincipalName").alias("user_principal_name"),
                    col("mail"),
                    col("companyName").alias("company_name"),
                    col("surname"),
                    col("displayName").alias("display_name"),
                    col("jobTitle").alias("job_title"),
                    col("accountEnabled").alias("account_enabled"),
                    col("userType").alias("account_type"),
                    col("onPremisesExtensionAttributes.extensionAttribute8").alias("primary_location"),
                    col("onPremisesExtensionAttributes.extensionAttribute9").alias("ciphr_primary_id"),
                    col("onPremisesExtensionAttributes.extensionAttribute10").alias("user_type"),
                    col("onPremisesExtensionAttributes.extensionAttribute14").alias("arbor_unique_id"),
                    col("onPremisesExtensionAttributes.extensionAttribute15").alias("mifare_card"),
                    col("onPremisesSamAccountName").alias("on_premise_sam_account_name"),
                    to_timestamp(col("signInActivity.lastSignInDateTime")).alias("last_sign_in_datetime"),
                    to_timestamp(col("signInActivity.lastNonInteractiveSignInDateTime")).alias("last_non_interactive_sign_in_datetime"),
                    to_timestamp(col("signInActivity.lastSuccessfulSignInDateTime")).alias("last_successful_sign_in_datetime"),
                    col("onPremisesSyncEnabled").alias("on_premise_sync_enabled")
                )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Get Teams Numbers

# CELL ********************

request_url = f"https://graph.microsoft.com/beta/admin/teams/userConfigurations?$select=id,userPrincipalName,telephoneNumbers&$filter=telephoneNumbers/any(t: t/assignmentCategory eq 'primary')"
df_team_phones = get_graph_api(request_url, request_headers, graph_api_phone_schema, "value")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Query pulls back only the primary assignment and users can only have one so no issues with duplicates.

df_phones_flat = (
    df_team_phones
    .withColumn("tel", explode("telephoneNumbers"))  # tel is the struct
    .select(
        col("id").alias("user_id"),
        col("userPrincipalName").alias("user_principal_name"),
        col("tel.telephoneNumber").alias("primary_teams_number"),
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Get Risk Status

# CELL ********************

request_url = f"https://graph.microsoft.com/v1.0/identityProtection/riskyUsers?$select=id,riskState,riskLevel,riskDetail,riskLastUpdatedDateTime"
df_risky_users = get_graph_api(request_url, request_headers, risky_users_schema, "value")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_risky_users_clean = (
    df_risky_users
    .select(
        col("id").alias("user_id"),
        col("riskState").alias("risk_state"),
        col("riskLevel").alias("risk_level"),
        col("riskDetail").alias("risk_detail"),
        to_timestamp(col("riskLastUpdatedDateTime")).alias("risk_last_updated_datetime")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Raw Write

# CELL ********************

df_users_final = (df_users_clean
                    .join(df_phones_flat, on=["user_id", "user_principal_name"], how="left")
                    .join(df_risky_users_clean, on="user_id", how="left")
                )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(df_users_final.write
    .mode("overwrite")
    .format("delta")
    #.option("mergeSchema", "true")
    .option("overwriteSchema", "true")
    .saveAsTable(raw_user_table_name)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Base Merge

# CELL ********************

df_raw = spark.read.table(raw_user_table_name)
df_raw = df_raw.withColumn("account_deleted", lit(False))
df_raw.createOrReplaceTempView("raw")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## EXAMPLE MIGRATION SCRIPT

#df_new = (
#    spark.read.table(base_user_table_name)
#      # drop old columns (if you want them gone now)
#      .drop(
#          "lastSignInDateTime",
#          "lastNonInteractiveSignInDateTime",
#          "lastSuccessfulSignInDateTime"
#      )
      # add new NULL timestamp columns
#      .withColumn(
#          "last_sign_in_datetime",
#          lit(None).cast(TimestampType())
#      )
#      .withColumn(
#          "last_non_interactive_sign_in_datetime",
#         lit(None).cast(TimestampType())
#      )
#      .withColumn(
#          "last_successful_sign_in_datetime",
#          lit(None).cast(TimestampType())
#      )
#)
#(df_new.write
#   .format("delta")
#   .mode("overwrite")
#   .option("overwriteSchema", "true")
#   .saveAsTable(base_user_table_name)
#)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

base_table_exists = spark.catalog.tableExists(base_user_table_name)

merge_condition = "base.user_id = raw.user_id"

if base_table_exists:
    spark.sql(f"""
        MERGE INTO {base_user_table_name} AS base
            USING raw AS raw
            ON {merge_condition}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            WHEN NOT MATCHED BY SOURCE THEN
            UPDATE SET
            account_enabled = false,
            account_deleted = true
        """)
else:
    df_raw.write.format("delta").saveAsTable(base_user_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
