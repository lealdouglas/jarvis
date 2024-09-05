# --------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import os
from azure_objects.credential import auth_credential
from azure.mgmt.eventhub import EventHubManagementClient
from azure.core.exceptions import ResourceNotFoundError
from utils.helper import validate_args
from utils.cons import SUBSCRIPTION_ID
from utils.logger import log_info, log_error


def get_eventhub_client() -> EventHubManagementClient:
    """
    Cria e retorna um cliente do Azure Event Hub Management.
    """
    return EventHubManagementClient(
        credential=auth_credential(), subscription_id=SUBSCRIPTION_ID
    )


def get_eventhub_name(properties: dict) -> str:
    """
    Gera e retorna o nome do Event Hub com base nas propriedades fornecidas.
    """
    return f"topic-{properties['DOMAIN']}-{properties['datacontract']['ingest_workflow']['model']}"


def get_container_name(properties: dict) -> str:
    """
    Gera e retorna o nome do container com base nas propriedades fornecidas.
    """
    return f"ctr{properties['DOMAIN']}raw"


def create_eventhub(
    eventhub_client: EventHubManagementClient,
    properties: dict,
    eventhub_name: str,
    container_name: str,
) -> None:
    """
    Cria um Event Hub com as configurações especificadas.
    """
    RESOURCE_GROUP_NAME = properties['RESOURCE_GROUP_NAME']
    STORAGE_ACCOUNT_NAME = properties['STORAGE_ACCOUNT_NAME']

    eventhub = eventhub_client.event_hubs.create_or_update(
        RESOURCE_GROUP_NAME,
        properties['EVENTHUB_NAMESPACE_NAME'],
        eventhub_name,
        {
            'message_retention_in_days': '1',
            'partition_count': '1',
            'status': 'Active',
            'capture_description': {
                'enabled': True,
                'encoding': 'Avro',
                'interval_in_seconds': '120',
                'size_limit_in_bytes': '10485763',
                'destination': {
                    'name': 'EventHubArchive.AzureBlockBlob',
                    'storage_account_resource_id': f'/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{RESOURCE_GROUP_NAME}/providers/Microsoft.Storage/storageAccounts/{STORAGE_ACCOUNT_NAME}',
                    'blob_container': container_name,
                    'archive_name_format': '{Namespace}/{EventHub}/{PartitionId}/{Year}/{Month}/{Day}/{Hour}/{Minute}/{Second}',
                },
            },
        },
    )
    log_info('Create EventHub: {}'.format(eventhub))


def get_or_create_eventhub(
    eventhub_client: EventHubManagementClient,
    properties: dict,
    eventhub_name: str,
) -> None:
    """
    Tenta obter o Event Hub, se não existir, cria um novo.
    """
    RESOURCE_GROUP_NAME = properties['RESOURCE_GROUP_NAME']
    EVENTHUB_NAMESPACE_NAME = properties['EVENTHUB_NAMESPACE_NAME']

    try:
        # Tenta obter o Event Hub
        eventhub = eventhub_client.event_hubs.get(
            RESOURCE_GROUP_NAME, EVENTHUB_NAMESPACE_NAME, eventhub_name
        )
        log_info('Topico Event Hub já existe: {}'.format(eventhub))
    except ResourceNotFoundError:
        # Cria o Event Hub se não existir
        create_eventhub(
            eventhub_client,
            properties,
            eventhub_name,
            get_container_name(properties),
        )
        # Obtém o Event Hub criado
        eventhub = eventhub_client.event_hubs.get(
            RESOURCE_GROUP_NAME, EVENTHUB_NAMESPACE_NAME, eventhub_name
        )
        log_info('Get EventHub: {}'.format(eventhub))


def create_event_hub_ingest(properties: dict) -> None:
    """
    Função principal para criar ou obter um Event Hub de ingestão com base nas propriedades fornecidas.

    Parâmetros:
    - properties (dict): Dicionário contendo as propriedades necessárias.
    """
    validate_args(
        [
            'RESOURCE_GROUP_NAME',
            'STORAGE_ACCOUNT_NAME',
            'EVENTHUB_NAMESPACE_NAME',
            'DOMAIN',
        ],
        properties,
    )

    eventhub_client = get_eventhub_client()
    eventhub_name = get_eventhub_name(properties)
    get_or_create_eventhub(eventhub_client, properties, eventhub_name)
