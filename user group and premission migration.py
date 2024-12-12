# Databricks notebook source
import requests
import json

# Configuration: Replace these with your workspace-specific details
DATABRICKS_INSTANCE = "https://dbc-48ae7f19-163c.cloud.databricks.com"  # Databricks workspace URL
TOKEN = "<your-databricks-token>"  # Bearer token for Databricks REST API
GROUP_API_URL = f"{DATABRICKS_INSTANCE}/api/2.0/groups"
USER_API_URL = f"{DATABRICKS_INSTANCE}/api/2.0/users"
PERMISSIONS_API_URL = f"{DATABRICKS_INSTANCE}/api/2.0/unity-catalog/permissions"

# Example: Hive to Unity Catalog mapping (you need to build this manually)
# Format: 'hive_group_name': ('unity_catalog_group_name', 'permissions')
hive_to_uc_mapping = {
    'data_scientists': ('data_scientists_uc', ['SELECT', 'INSERT']),
    'data_engineers': ('data_engineers_uc', ['SELECT', 'UPDATE']),
}

# Function to get the Unity Catalog group ID by group name
def get_uc_group_id(group_name):
    response = requests.get(
        GROUP_API_URL, 
        headers={"Authorization": f"Bearer {TOKEN}"}
    )
    groups = response.json().get('groups', [])
    
    for group in groups:
        if group['display_name'] == group_name:
            return group['id']
    return None

# Function to get Unity Catalog schema ID by schema name
def get_uc_schema_id(catalog_name, schema_name):
    response = requests.get(
        f"{DATABRICKS_INSTANCE}/api/2.0/unity-catalog/schemas/{catalog_name}/{schema_name}",
        headers={"Authorization": f"Bearer {TOKEN}"}
    )
    return response.json().get('id')

# Function to grant permissions in Unity Catalog
def grant_permissions_on_schema(group_id, schema_id, permissions):
    for permission in permissions:
        payload = {
            "group_id": group_id,
            "object_id": schema_id,
            "permission": permission
        }
        response = requests.post(
            f"{PERMISSIONS_API_URL}/schemas/{schema_id}/grants",
            headers={"Authorization": f"Bearer {TOKEN}"},
            json=payload
        )
        if response.status_code == 200:
            print(f"Successfully granted {permission} to group {group_id} on schema {schema_id}")
        else:
            print(f"Failed to grant {permission} to group {group_id} on schema {schema_id}: {response.text}")

# Main migration process
def migrate_permissions():
    for hive_group, (uc_group, permissions) in hive_to_uc_mapping.items():
        print(f"Processing group {hive_group} -> Unity Catalog group {uc_group}")

        # Get Unity Catalog group ID
        uc_group_id = get_uc_group_id(uc_group)
        if not uc_group_id:
            print(f"Unity Catalog group '{uc_group}' not found!")
            continue

        # Iterate over your schemas (You need to specify which schemas this group should access)
        catalogs = ["my_catalog"]  # Example catalog names
        schemas = ["sales_data", "finance_data"]  # Example schema names

        for catalog in catalogs:
            for schema in schemas:
                print(f"Granting permissions on schema '{schema}' in catalog '{catalog}' to group '{uc_group}'")
                schema_id = get_uc_schema_id(catalog, schema)
                if schema_id:
                    grant_permissions_on_schema(uc_group_id, schema_id, permissions)
                else:
                    print(f"Schema '{schema}' not found in catalog '{catalog}'.")

if __name__ == "__main__":
    migrate_permissions()


# COMMAND ----------

import requests
import json

# Configuration: Replace these with your workspace-specific details
DATABRICKS_INSTANCE = "https://<databricks-instance>"  # Databricks workspace URL
TOKEN = "<your-databricks-token>"  # Bearer token for Databricks REST API
GROUP_API_URL = f"{DATABRICKS_INSTANCE}/api/2.0/groups"
USER_API_URL = f"{DATABRICKS_INSTANCE}/api/2.0/users"
PERMISSIONS_API_URL = f"{DATABRICKS_INSTANCE}/api/2.0/unity-catalog/permissions"

# Example: Hive to Unity Catalog mapping (you need to build this manually)
# Format: 'hive_group_name': ('unity_catalog_group_name', 'permissions')
hive_to_uc_mapping = {
    'data_scientists': ('data_scientists_uc', ['SELECT', 'INSERT']),
    'data_engineers': ('data_engineers_uc', ['SELECT', 'UPDATE']),
}

# Function to get the Unity Catalog group ID by group name
def get_uc_group_id(group_name):
    response = requests.get(
        GROUP_API_URL, 
        headers={"Authorization": f"Bearer {TOKEN}"}
    )
    groups = response.json().get('groups', [])
    
    for group in groups:
        if group['display_name'] == group_name:
            return group['id']
    return None

# Function to get Unity Catalog schema ID by schema name
def get_uc_schema_id(catalog_name, schema_name):
    response = requests.get(
        f"{DATABRICKS_INSTANCE}/api/2.0/unity-catalog/schemas/{catalog_name}/{schema_name}",
        headers={"Authorization": f"Bearer {TOKEN}"}
    )
    return response.json().get('id')

# Function to grant permissions in Unity Catalog
def grant_permissions_on_schema(group_id, schema_id, permissions):
    for permission in permissions:
        payload = {
            "group_id": group_id,
            "object_id": schema_id,
            "permission": permission
        }
        response = requests.post(
            f"{PERMISSIONS_API_URL}/schemas/{schema_id}/grants",
            headers={"Authorization": f"Bearer {TOKEN}"},
            json=payload
        )
        if response.status_code == 200:
            print(f"Successfully granted {permission} to group {group_id} on schema {schema_id}")
        else:
            print(f"Failed to grant {permission} to group {group_id} on schema {schema_id}: {response.text}")

# Main migration process
def migrate_permissions():
    for hive_group, (uc_group, permissions) in hive_to_uc_mapping.items():
        print(f"Processing group {hive_group} -> Unity Catalog group {uc_group}")

        # Get Unity Catalog group ID
        uc_group_id = get_uc_group_id(uc_group)
        if not uc_group_id:
            print(f"Unity Catalog group '{uc_group}' not found!")
            continue

        # Iterate over your schemas (You need to specify which schemas this group should access)
        catalogs = ["my_catalog"]  # Example catalog names
        schemas = ["sales_data", "finance_data"]  # Example schema names

        for catalog in catalogs:
            for schema in schemas:
                print(f"Granting permissions on schema '{schema}' in catalog '{catalog}' to group '{uc_group}'")
                schema_id = get_uc_schema_id(catalog, schema)
                if schema_id:
                    grant_permissions_on_schema(uc_group_id, schema_id, permissions)
                else:
                    print(f"Schema '{schema}' not found in catalog '{catalog}'.")

if __name__ == "__main__":
    migrate_permissions()

