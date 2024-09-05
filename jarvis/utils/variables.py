import os
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.databricks import AzureDatabricksManagementClient
from azure_objects.credential import auth_credential
from databricks_objects.credential import work_credential
from utils.cons import SUBSCRIPTION_ID, validate_args
from utils.logger import log_error, log_info


def get_resource_group_name(client, domain: str) -> str:
    """
    Recupera o nome do grupo de recursos que contém a nomenclatura especificada.
    """
    group_list = client.resource_groups.list()
    for group in list(group_list):
        if domain in group.name:
            return group.name
    return None


def get_resources(client, resource_group_name: str) -> list:
    """
    Recupera a lista de recursos dentro do grupo de recursos especificado.
    """
    return client.resources.list_by_resource_group(
        resource_group_name, expand='createdTime,changedTime'
    )


def get_resource_names(resources: list) -> dict:
    """
    Identifica e armazena os nomes dos recursos do tipo EventHub Namespace, Storage Account e Databricks Workspace.
    """
    resource_names = {
        'EVENTHUB_NAMESPACE_NAME': None,
        'STORAGE_ACCOUNT_NAME': None,
        'WORKSPACE_ADB_NAME': None,
    }
    for resource in list(resources):
        if resource.type == 'Microsoft.EventHub/namespaces':
            resource_names['EVENTHUB_NAMESPACE_NAME'] = str(resource.name)
        elif resource.type == 'Microsoft.Storage/storageAccounts':
            resource_names['STORAGE_ACCOUNT_NAME'] = str(resource.name)
        elif resource.type == 'Microsoft.Databricks/workspaces':
            resource_names['WORKSPACE_ADB_NAME'] = str(resource.name)
    return resource_names


def set_databricks_workspace_url(
    client, resource_group_name: str, workspace_name: str
):
    """
    Recupera a URL do workspace do Databricks e define a variável de ambiente 'HOST'.
    """
    response = client.workspaces.get(
        resource_group_name=resource_group_name,
        workspace_name=workspace_name,
    )
    os.environ['HOST'] = str(response.workspace_url)


def get_cluster_id(work_credential, domain: str) -> str:
    """
    Recupera a lista de clusters do Databricks e encontra o cluster que contém a nomenclatura especificada.
    """
    for cluster in work_credential.clusters.list():
        if domain in cluster.cluster_name:
            return str(cluster.cluster_id)
    return None


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
    - properties (dict): Dicionário contendo as propriedades necessárias.

    Retorno:
    - dict: Um dicionário contendo os nomes dos recursos encontrados.
    """
    validate_args(['DOMAIN'], properties)

    # Cria um cliente do Azure Resource Management
    resource_client = ResourceManagementClient(
        credential=auth_credential(), subscription_id=SUBSCRIPTION_ID
    )

    # Recupera o nome do grupo de recursos
    resource_group_name = get_resource_group_name(
        resource_client, properties['DOMAIN']
    )

    # Recupera a lista de recursos dentro do grupo de recursos encontrado
    resources = get_resources(resource_client, resource_group_name)

    # Identifica e armazena os nomes dos recursos
    resource_names = get_resource_names(resources)

    # Cria um cliente do Azure Databricks Management
    databricks_client = AzureDatabricksManagementClient(
        credential=auth_credential(), subscription_id=SUBSCRIPTION_ID
    )

    # Recupera a URL do workspace do Databricks e define a variável de ambiente 'HOST'
    set_databricks_workspace_url(
        databricks_client,
        resource_group_name,
        resource_names['WORKSPACE_ADB_NAME'],
    )

    # Recupera o ID do cluster do Databricks
    cluster_id = get_cluster_id(work_credential(), properties['DOMAIN'])

    # Retorna um dicionário com os nomes dos recursos encontrados
    return {
        **properties,
        'RESOURCE_GROUP_NAME': resource_group_name,
        'EVENTHUB_NAMESPACE_NAME': resource_names['EVENTHUB_NAMESPACE_NAME'],
        'STORAGE_ACCOUNT_NAME': resource_names['STORAGE_ACCOUNT_NAME'],
        'WORKSPACE_ADB_NAME': resource_names['WORKSPACE_ADB_NAME'],
        'CLUSTER_ID': cluster_id,
    }
