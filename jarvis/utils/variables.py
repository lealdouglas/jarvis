import os
import importlib
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.databricks import AzureDatabricksManagementClient
from azure_objects.credential import auth_credential
from databricks_objects.credential import work_credential
from utils.cons import *
from utils.logger import log_error, log_info


def variables(properties: dict) -> dict:
    """
    Função para obter variáveis importantes relacionadas aos recursos do Azure.

    Esta função realiza as seguintes operações:
    1. Inicializa variáveis para armazenar nomes de recursos.
    2. Cria um cliente do Azure Resource Management.
    3. Recupera a lista de grupos de recursos e encontra o grupo de recursos que contém a nomenclatura especificada.
    4. Recupera a lista de recursos dentro do grupo de recursos encontrado.
    5. Identifica e armazena os nomes dos recursos do tipo EventHub Namespace, Storage Account e Databricks Workspace.
    6. Cria um cliente do Azure Databricks Management.
    7. Recupera a URL do workspace do Databricks e define a variável de ambiente 'HOST'.
    8. Recupera a lista de clusters do Databricks e encontra o cluster que contém a nomenclatura especificada.
    9. Retorna um dicionário com os nomes dos recursos encontrados.

    Parâmetros:
    - domain (str): O dominio usada para identificar os recursos.

    Retorno:
    - dict[str, str]: Um dicionário contendo os nomes dos recursos encontrados.

    Exemplo de uso:
    ```python
    variaveis = variables("domain")
    print(variaveis)
    ```
    """

    validate_args(
        [
            'DOMAIN',
        ],
        properties,
    )

    # Inicializa variáveis para armazenar nomes de recursos
    RESOURCE_GROUP_NAME = None
    EVENTHUB_NAMESPACE_NAME = None
    STORAGE_ACCOUNT_NAME = None
    WORKSPACE_ADB_NAME = None
    CLUSTER_ID = None

    # Cria um cliente do Azure Resource Management
    client = ResourceManagementClient(
        credential=auth_credential(), subscription_id=SUBSCRIPTION_ID
    )

    # Recupera a lista de grupos de recursos
    group_list = client.resource_groups.list()

    # Encontra o grupo de recursos que contém a nomenclatura especificada
    for group in list(group_list):
        if properties['DOMAIN'] in group.name:
            RESOURCE_GROUP_NAME = group.name
            break

    # Recupera a lista de recursos dentro do grupo de recursos encontrado
    resource_list = client.resources.list_by_resource_group(
        RESOURCE_GROUP_NAME, expand='createdTime,changedTime'
    )

    # Identifica e armazena os nomes dos recursos do tipo EventHub Namespace, Storage Account e Databricks Workspace
    for resource in list(resource_list):
        EVENTHUB_NAMESPACE_NAME = (
            str(resource.name)
            if resource.type == 'Microsoft.EventHub/namespaces'
            else EVENTHUB_NAMESPACE_NAME
        )
        STORAGE_ACCOUNT_NAME = (
            str(resource.name)
            if resource.type == 'Microsoft.Storage/storageAccounts'
            else STORAGE_ACCOUNT_NAME
        )
        WORKSPACE_ADB_NAME = (
            str(resource.name)
            if resource.type == 'Microsoft.Databricks/workspaces'
            else WORKSPACE_ADB_NAME
        )

    # Cria um cliente do Azure Databricks Management
    client = AzureDatabricksManagementClient(
        credential=auth_credential(), subscription_id=SUBSCRIPTION_ID
    )

    # Recupera a URL do workspace do Databricks e define a variável de ambiente 'HOST'
    response = client.workspaces.get(
        resource_group_name=RESOURCE_GROUP_NAME,
        workspace_name=WORKSPACE_ADB_NAME,
    )

    os.environ['HOST'] = str(response.workspace_url)

    # Recupera a lista de clusters do Databricks e encontra o cluster que contém a nomenclatura especificada
    for cluster in work_credential().clusters.list():
        CLUSTER_ID = (
            str(cluster.cluster_id)
            if properties['DOMAIN'] in cluster.cluster_name
            else CLUSTER_ID
        )

    # Retorna um dicionário com os nomes dos recursos encontrados
    return {
        **properties,
        'RESOURCE_GROUP_NAME': RESOURCE_GROUP_NAME,
        'EVENTHUB_NAMESPACE_NAME': EVENTHUB_NAMESPACE_NAME,
        'STORAGE_ACCOUNT_NAME': STORAGE_ACCOUNT_NAME,
        'WORKSPACE_ADB_NAME': WORKSPACE_ADB_NAME,
        'CLUSTER_ID': CLUSTER_ID,
    }


# import os
# import importlib
# from azure.mgmt.resource import ResourceManagementClient
# from azure.mgmt.databricks import AzureDatabricksManagementClient
# from azure_objects.credential import auth_credential
# from databricks_objects.credential import work_credential
# from utils.cons import *
# from utils.logger import log_error, log_info


# def variables(properties['DOMAIN']: str) -> dict[str, str]:
#     # Listar todos os arquivos na pasta

#     RESOURCE_GROUP_NAME = None
#     EVENTHUB_NAMESPACE_NAME = None
#     STORAGE_ACCOUNT_NAME = None
#     WORKSPACE_ADB_NAME = None
#     CLUSTER_ID = None

#     client = ResourceManagementClient(
#         credential=auth_credential(), subscription_id=SUBSCRIPTION_ID
#     )

#     # Retrieve the list of resource groups
#     group_list = client.resource_groups.list()

#     # Show the groups in formatted output
#     column_width = 40

#     # print("Resource Group".ljust(column_width) + "Location")
#     # print("-" * (column_width * 2))

#     # EVENTHUB_NAMESPACE_NAME = [str(resource.name) for resource in list(resource_list) if resource.type == properties['DOMAIN'] in group.name]
#     for group in list(group_list):
#         if properties['DOMAIN'] in group.name:
#             RESOURCE_GROUP_NAME = group.name
#             break

#     # Retrieve the list of resources in "myResourceGroup" (change to any name desired).
#     # The expand argument includes additional properties in the output.
#     resource_list = client.resources.list_by_resource_group(
#         RESOURCE_GROUP_NAME, expand='createdTime,changedTime'
#     )

#     for resource in list(resource_list):
#         EVENTHUB_NAMESPACE_NAME = (
#             str(resource.name)
#             if resource.type == 'Microsoft.EventHub/namespaces'
#             else EVENTHUB_NAMESPACE_NAME
#         )
#         STORAGE_ACCOUNT_NAME = (
#             str(resource.name)
#             if resource.type == 'Microsoft.Storage/storageAccounts'
#             else STORAGE_ACCOUNT_NAME
#         )
#         WORKSPACE_ADB_NAME = (
#             str(resource.name)
#             if resource.type == 'Microsoft.Databricks/workspaces'
#             else WORKSPACE_ADB_NAME
#         )

#     client = AzureDatabricksManagementClient(
#         credential=auth_credential(), subscription_id=SUBSCRIPTION_ID
#     )

#     response = client.workspaces.get(
#         resource_group_name=RESOURCE_GROUP_NAME,
#         workspace_name=WORKSPACE_ADB_NAME,
#     )

#     os.environ['HOST'] = str(response.workspace_url)
#     for cluster in work_credential().clusters.list():
#         CLUSTER_ID = (
#             str(cluster.cluster_id)
#             if properties['DOMAIN'] in cluster.cluster_name
#             else CLUSTER_ID
#         )

#     return {
#         'RESOURCE_GROUP_NAME': RESOURCE_GROUP_NAME,
#         'EVENTHUB_NAMESPACE_NAME': EVENTHUB_NAMESPACE_NAME,
#         'STORAGE_ACCOUNT_NAME': STORAGE_ACCOUNT_NAME,
#         'WORKSPACE_ADB_NAME': WORKSPACE_ADB_NAME,
#         'CLUSTER_ID': CLUSTER_ID,
#     }

#     # log_info(eventhub_namespace_name)
#     # log_info(eventhub_namespace_name)
#     # log_info(storage_account_name)
#     # log_info(workspace_adb_name)
#     # log_info(workspace_url)
#     # log_info(cluster_id)

#     # # Show the groups in formatted output
#     # column_width = 36

#     # # print("Resource".ljust(column_width) + "Type".ljust(column_width)
#     # #     + "Create date".ljust(column_width) + "Change date".ljust(column_width))
#     # # print("-" * (column_width * 4))

#     # eventhub_namespace = None
#     # eventhub_namespace_name = None
#     # for resource in list(resource_list):
#     #     # print(f"{resource.name:<{column_width}}{resource.type:<{column_width}}"
#     #     #     f"{str(resource.created_time):<{column_width}}{str(resource.changed_time):<{column_width}}")
#     #     if 'Microsoft.EventHub/namespaces' == resource.type:
#     #         eventhub_namespace_name = resource.name
#     #     if 'Microsoft.Storage/storageAccounts' == resource.type:
#     #         storage_account_name = resource.name
#     #     if 'Microsoft.Databricks/workspaces' == resource.type:
#     #         workspace_adb_name = resource.name

#     # client = AzureDatabricksManagementClient(
#     #     credential=auth_credential(), subscription_id=SUBSCRIPTION_ID
#     # )

#     # response = client.workspaces.get(
#     #     resource_group_name=RESOURCE_GROUP_NAME,
#     #     workspace_name=workspace_adb_name,
#     # )

#     # workspace_url = response.workspace_url

#     # os.environ['HOST'] = str(workspace_url)
#     # w = work_credential()

#     # for cluster in w.clusters.list():
#     #     if 'strife' in cluster.cluster_name:
#     #         cluster_id = cluster.cluster_id

#     # log_info('Variaveis importantes'.ljust(column_width) + 'valor')
#     # log_info('-' * (column_width * 2))
#     # log_info(eventhub_namespace_name)
#     # log_info(eventhub_namespace_name)
#     # log_info(storage_account_name)
#     # log_info(workspace_adb_name)
#     # log_info(workspace_url)
#     # log_info(cluster_id)
