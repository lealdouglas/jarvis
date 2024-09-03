import os
import importlib
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.databricks import AzureDatabricksManagementClient
from azure_objects.credential import auth_credential
from databricks_objects.credential import work_credential
from utils.cons import *
from utils.logger import log_error, log_info


def run_actions():
    # Listar todos os arquivos na pasta

    client = ResourceManagementClient(
        credential=auth_credential(), subscription_id=SUBSCRIPTION_ID
    )

    # Retrieve the list of resource groups
    group_list = client.resource_groups.list()

    # Show the groups in formatted output
    column_width = 40

    # print("Resource Group".ljust(column_width) + "Location")
    # print("-" * (column_width * 2))

    # for group in list(group_list):
    #     print(f"{group.name:<{column_width}}{group.location}")

    # Retrieve the list of resources in "myResourceGroup" (change to any name desired).
    # The expand argument includes additional properties in the output.
    resource_list = client.resources.list_by_resource_group(
        RESOURCE_GROUP_NAME, expand='createdTime,changedTime'
    )

    # Show the groups in formatted output
    column_width = 36

    # print("Resource".ljust(column_width) + "Type".ljust(column_width)
    #     + "Create date".ljust(column_width) + "Change date".ljust(column_width))
    # print("-" * (column_width * 4))

    eventhub_namespace = None
    eventhub_namespace_name = None
    for resource in list(resource_list):
        # print(f"{resource.name:<{column_width}}{resource.type:<{column_width}}"
        #     f"{str(resource.created_time):<{column_width}}{str(resource.changed_time):<{column_width}}")
        if 'Microsoft.EventHub/namespaces' == resource.type:
            eventhub_namespace_name = resource.name
        if 'Microsoft.Storage/storageAccounts' == resource.type:
            storage_account_name = resource.name
        if 'Microsoft.Databricks/workspaces' == resource.type:
            workspace_adb_name = resource.name

    client = AzureDatabricksManagementClient(
        credential=auth_credential(), subscription_id=SUBSCRIPTION_ID
    )

    response = client.workspaces.get(
        resource_group_name=RESOURCE_GROUP_NAME,
        workspace_name=workspace_adb_name,
    )

    workspace_url = response.workspace_url

    os.environ['HOST'] = str(workspace_url)
    w = work_credential()

    for cluster in w.clusters.list():
        if 'strife' in cluster.cluster_name:
            cluster_id = cluster.cluster_id

    log_info('Variaveis importantes'.ljust(column_width) + 'valor')
    log_info('-' * (column_width * 2))
    log_info(eventhub_namespace_name)
    log_info(eventhub_namespace_name)
    log_info(storage_account_name)
    log_info(workspace_adb_name)
    log_info(workspace_url)
    log_info(cluster_id)


if __name__ == '__main__':
    run_actions()
