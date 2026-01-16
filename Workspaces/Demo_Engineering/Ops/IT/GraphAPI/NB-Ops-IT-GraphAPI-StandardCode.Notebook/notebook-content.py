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

# # Standard Code for Microsoft Graph API

# MARKDOWN ********************

# ## Initial Config

# CELL ********************

from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql.functions import to_timestamp, to_date, col, date_format, lit, current_date, date_sub, explode
from delta.tables import *
from datetime import datetime, timedelta
from functools import reduce
import msal
import requests
import json
import time
import random

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Get API Secret

# CELL ********************

# Requires an Azure Key Vault. Owner of the notebook needs read permission on secret.
AKVUrl = "https://altakv01.vault.azure.net/"
AKVSecretName = "GraphAPI-AttackSimulation"
client_secret = mssparkutils.credentials.getSecret(AKVUrl, AKVSecretName)

#Details of App Registration that the client secret belongs to.
tenant_id = "23990c89-ae67-4afc-87ce-470c97afeb37"
client_id = "67b0a96f-4127-4267-ac91-887b1a95fa54"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Standard Functions

# MARKDOWN ********************

# ### Handle getting an access token

# CELL ********************

def get_access_token(tenant_id, client_id, client_secret):
    authority_url = f'https://login.microsoftonline.com/{tenant_id}/'
    scope = ["https://graph.microsoft.com/.default"]
    
    app = msal.ConfidentialClientApplication(
        client_id,
        authority=authority_url,
        validate_authority=True,
        client_credential=client_secret
    )
    
    result = app.acquire_token_for_client(scopes=scope)
    
    if 'access_token' in result:
        return result['access_token']
    else:
        print('Error in getAccessToken:', result.get("error"), result.get("error_description"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Communication with API

# CELL ********************

def get_contents_old(api_url, headers, schema, json_top_element="value"):
    """Fetch pages from the API and yield as DataFrames."""
    session = requests.Session()
    more_pages = True
    url = api_url

    while more_pages:
        try:
            response = session.get(url, headers=headers).json()
            # Pagination management
            url = response.get("@odata.nextLink")
            if url is None: 
                more_pages = False 
            else: 
                more_pages = True
            yield spark.createDataFrame(response[json_top_element], schema=schema)

        except Exception as e:
            # logging.error("Unable to retrieve response: %s", e)
            print(e)
            more_pages = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_contents(api_url, headers, schema, json_top_element="value",
                 timeout=60, max_retries=8, base_backoff=1.0, max_backoff=60.0):
    session = requests.Session()
    url = api_url

    while url:
        retries = 0

        while True:
            resp = session.get(url, headers=headers, timeout=timeout)

            # Handle throttling
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                if retry_after is not None:
                    sleep_s = float(retry_after)
                else:
                    # Exponential backoff with jitter
                    sleep_s = min(max_backoff, base_backoff * (2 ** retries))
                    sleep_s = sleep_s * (0.7 + 0.6 * random.random())  # jitter 0.7–1.3x

                retries += 1
                if retries > max_retries:
                    raise PermissionError(
                        f"Graph throttling persisted after {max_retries} retries. "
                        f"Last response: HTTP 429. Body: {resp.text[:1000]}"
                    )

                time.sleep(sleep_s)
                continue

            # Parse JSON
            raw_text = resp.text
            try:
                data = resp.json()
            except Exception:
                raise RuntimeError(
                    f"Graph returned non-JSON response. HTTP {resp.status_code}. "
                    f"Body: {raw_text[:2000]}"
                )

            # Fail fast on other non-2xx errors
            if not resp.ok:
                err = data.get("error", {})
                code = err.get("code", "UNKNOWN")
                msg = err.get("message", raw_text)
                raise RuntimeError(
                    f"Graph call failed. HTTP {resp.status_code}. "
                    f"Error code: {code}. Message: {msg}"
                )

            # Success
            items = data.get(json_top_element, [])
            yield spark.createDataFrame(items, schema=schema)
            url = data.get("@odata.nextLink")
            break
        url = data.get("@odata.nextLink")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Combine responses into single data frame

# CELL ********************

def get_graph_api_old(api_url, request_headers, schema, json_top_element="value"):
    """Aggregate pages from the API into a single DataFrame."""
    
    # Collect all pages into a list of DataFrames
    pages = list(get_contents(api_url, request_headers, schema, json_top_element=json_top_element))
    
    if pages:
        # Efficiently union multiple DataFrames using reduce
        df = reduce(lambda x, y: x.union(y), pages)
    else:
        # Create an empty DataFrame if no pages
        df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema=schema)

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_graph_api(api_url, request_headers, schema, json_top_element="value"):
    pages = list(get_contents(api_url, request_headers, schema, json_top_element=json_top_element))

    if pages:
        return reduce(lambda x, y: x.union(y), pages)

    return spark.createDataFrame(spark.sparkContext.emptyRDD(), schema=schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
