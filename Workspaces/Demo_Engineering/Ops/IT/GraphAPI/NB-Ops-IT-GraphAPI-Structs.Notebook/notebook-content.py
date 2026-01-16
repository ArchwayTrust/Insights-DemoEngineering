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
# META     }
# META   }
# META }

# MARKDOWN ********************

# # PySpark Structs for Graph API

# CELL ********************

from pyspark.sql.types import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Graph API Users

# CELL ********************

on_premise_extension_attributes_schema = StructType([
    StructField("extensionAttribute1", StringType(), True),
    StructField("extensionAttribute2", StringType(), True),
    StructField("extensionAttribute3", StringType(), True),
    StructField("extensionAttribute4", StringType(), True),
    StructField("extensionAttribute5", StringType(), True),
    StructField("extensionAttribute6", StringType(), True),
    StructField("extensionAttribute7", StringType(), True),
    StructField("extensionAttribute8", StringType(), True),
    StructField("extensionAttribute9", StringType(), True),
    StructField("extensionAttribute10", StringType(), True),
    StructField("extensionAttribute11", StringType(), True),
    StructField("extensionAttribute12", StringType(), True),
    StructField("extensionAttribute13", StringType(), True),
    StructField("extensionAttribute14", StringType(), True),
    StructField("extensionAttribute15", StringType(), True),
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sign_in_activity_schema = StructType([
    StructField("lastSignInDateTime", StringType(), True),
    StructField("lastNonInteractiveSignInDateTime", StringType(), True),
    StructField("lastSuccessfulSignInDateTime", StringType(), True),
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

graph_api_user_schema = StructType([
    StructField("id", StringType(), True),
    StructField("employeeId", StringType(), True),
    StructField("userPrincipalName", StringType(), True),
    StructField("mail", StringType(), True),
    StructField("companyName", StringType(), True),
    StructField("surname", StringType(), True),
    StructField("displayName", StringType(), True),
    StructField("jobTitle", StringType(), True),
    StructField("accountEnabled", BooleanType(), True),
    StructField("userType", StringType(), True),
    StructField("onPremisesExtensionAttributes", on_premise_extension_attributes_schema, True),
    StructField("onPremisesSamAccountName", StringType(), True),
    StructField("signInActivity", sign_in_activity_schema, True),
    StructField("onPremisesSyncEnabled", StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Teams Phone Numbers

# CELL ********************

graph_api_phone_schema = StructType([
    StructField("id", StringType(), nullable=False),
    StructField("userPrincipalName", StringType(), nullable=False),
    StructField(
        "telephoneNumbers",
        ArrayType(
            StructType([
                StructField("telephoneNumber", StringType(), nullable=False),
                StructField("assignmentCategory", StringType(), nullable=False)
            ])
        ),
        nullable=True
    )
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Risky Users

# CELL ********************

risky_users_schema = StructType([
    StructField("id", StringType(), True),                       # User object id
    StructField("riskState", StringType(), True),                # atRisk, remediated, dismissed, etc.
    StructField("riskLevel", StringType(), True),                # low, medium, high, hidden, none
    StructField("riskDetail", StringType(), True),               # adminConfirmedUserCompromised, etc.
    StructField("riskLastUpdatedDateTime", StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Simulations List

# CELL ********************

# Define the structure for the user information fields in 'createdBy' and 'lastModifiedBy'
simulation_user_info_struct = StructType([
    StructField('email', StringType(), True),
    StructField('id', StringType(), True),
    StructField('displayName', StringType(), True)
])

# Define the main structure of the simulation items in 'value'
simulation_item_struct = StructType([
    StructField('id', StringType(), True),
    StructField('displayName', StringType(), True),
    StructField('description', StringType(), True),
    StructField('attackType', StringType(), True),
    StructField('payloadDeliveryPlatform', StringType(), True),
    StructField('attackTechnique', StringType(), True),
    StructField('status', StringType(), True),
    StructField('createdDateTime', StringType(), True),
    StructField('lastModifiedDateTime', StringType(), True),
    StructField('launchDateTime', StringType(), True),
    StructField('completionDateTime', StringType(), True),
    StructField('isAutomated', BooleanType(), True),
    StructField('automationId', StringType(), True),
    StructField('durationInDays', IntegerType(), True),
    StructField('trainingSetting', StringType(), True),
    StructField('oAuthConsentAppDetail', StringType(), True),
    StructField('endUserNotificationSetting', StringType(), True),
    StructField('includedAccountTarget', StringType(), True),
    StructField('excludedAccountTarget', StringType(), True),
    StructField('createdBy', simulation_user_info_struct, True),
    StructField('lastModifiedBy', simulation_user_info_struct, True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Simulation Users (Results)

# CELL ********************

# Define the sub-structure for the simulation user
simulation_results_user_schema = StructType([
    StructField("userId", StringType(), True),
    StructField("displayName", StringType(), True),
    StructField("email", StringType(), True)
])

# Define the sub-structure for the simulation events
simulation_results_event_schema = StructType([
    StructField("eventName", StringType(), True),
    StructField("eventDateTime", StringType(), True),
    StructField("ipAddress", StringType(), True),
    StructField("osPlatformDeviceDetails", StringType(), True),
    StructField("browser", StringType(), True)
])

# Define the sub-structure for training properties
simulation_results_training_properties_schema = StructType([
    StructField("contentDateTime", StringType(), True),
    StructField("ipAddress", StringType(), True),
    StructField("osPlatformDeviceDetails", StringType(), True),
    StructField("browser", StringType(), True),
    StructField("potentialScoreImpact", FloatType(), True)
])

# Define the sub-structure for the training events
simulation_results_training_event_schema = StructType([
    StructField("displayName", StringType(), True),
    StructField("latestTrainingStatus", StringType(), True),
    StructField("trainingAssignedProperties", simulation_results_training_properties_schema, True),
    StructField("trainingUpdatedProperties", simulation_results_training_properties_schema, True),
    StructField("trainingCompletedProperties", simulation_results_training_properties_schema, True)
])

# Now define the main value structure, using the sub-structures
simulation_user_value_schema = StructType([
    StructField("isCompromised", BooleanType(), True),
    StructField("compromisedDateTime", StringType(), True),
    StructField("assignedTrainingsCount", IntegerType(), True),
    StructField("completedTrainingsCount", IntegerType(), True),
    StructField("inProgressTrainingsCount", IntegerType(), True),
    StructField("reportedPhishDateTime", StringType(), True),
    StructField("simulationUser", simulation_results_user_schema, True),
    StructField("simulationEvents", ArrayType(simulation_results_event_schema), True),
    StructField("trainingEvents", ArrayType(simulation_results_training_event_schema), True)
])

# Same as above but with the simulation_id column for the delta table
simulation_user_schema = StructType([
    StructField("isCompromised", BooleanType(), True),
    StructField("compromisedDateTime", StringType(), True),
    StructField("assignedTrainingsCount", IntegerType(), True),
    StructField("completedTrainingsCount", IntegerType(), True),
    StructField("inProgressTrainingsCount", IntegerType(), True),
    StructField("reportedPhishDateTime", StringType(), True),
    StructField("simulationUser", simulation_results_user_schema, True),
    StructField("simulationEvents", ArrayType(simulation_results_event_schema), True),
    StructField("trainingEvents", ArrayType(simulation_results_training_event_schema), True),
    StructField("simulation_id", StringType(), True),
])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
