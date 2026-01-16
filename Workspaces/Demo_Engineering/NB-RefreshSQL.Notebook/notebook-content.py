# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Refresh SQL Endpoint for named workspace
# Mostly borrowed from https://github.com/microsoft/fabric-toolbox/blob/main/samples/notebook-refresh-tables-in-sql-endpoint/README.md

# CELL ********************

import json 
import notebookutils 
import sempy.fabric as fabric 
from sempy.fabric.exceptions import FabricHTTPException, WorkspaceNotFoundException 
from notebookutils import variableLibrary


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Get the entire variable library by name
vl = variableLibrary.getLibrary("VL_CORE")

# Fetch key environment variables
STORE_WORKSPACE_NAME = vl.getVariable("STORE_WORKSPACE_NAME")
STORE_WORKSPACE_ID = vl.getVariable("STORE_WORKSPACE_ID")
BASE_LH_NAME = vl.getVariable("BASE_LH_NAME")
RAW_LH_NAME = vl.getVariable("RAW_LH_NAME")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

BASE_LH_NAME

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

STORE_WORKSPACE_ID

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

lakehouse_data = notebookutils.lakehouse.getWithProperties(name=BASE_LH_NAME, workspaceId=STORE_WORKSPACE_ID)
lakehouse_id = lakehouse_data['id']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

lakehouse_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

#Instantiate the client
client = fabric.FabricRestClient()

# This is the SQL endpoint I want to sync with the lakehouse, this needs to be the GUI
sqlendpoint = fabric.FabricRestClient().get(f"/v1/workspaces/{STORE_WORKSPACE_ID}/lakehouses/{lakehouse_id}").json()['properties']['sqlEndpointProperties']['id']

# URI for the call 
uri = f"v1/workspaces/{STORE_WORKSPACE_ID}/sqlEndpoints/{sqlendpoint}/refreshMetadata" 

# This is the action, we want to take 
# payload = {} # empty payload, all tables
# Example of setting a timeout
payload = { "timeout": {"timeUnit": "Seconds", "value": "60"}  }  # Setting a timeout for the REST call

try:
    response = client.post(uri,json= payload, lro_wait = True) 
    sync_status = json.loads(response.text)
    display(sync_status)
except Exception as e: print(e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
