# --------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import os

from azure.core.exceptions import ResourceNotFoundError
from azure.mgmt.eventhub import EventHubManagementClient

from jarvis.azure_objects.credential import auth_credential
from jarvis.utils.cons import SUBSCRIPTION_ID
from jarvis.utils.helper import validate_args
from jarvis.utils.logger import log_error, log_info


def get_eventhub_client() -> EventHubManagementClient:
    """
    Cria e retorna um cliente do Azure Event Hub Management.
    Creates and returns an Azure Event Hub Management client.
    """
    return EventHubManagementClient(
        credential=auth_credential(), subscription_id=SUBSCRIPTION_ID
    )


def get_eventhub_name(properties: dict) -> dict:
    """
    Gera e retorna o nome do Event Hub com base nas propriedades fornecidas.
    Generates and returns the Event Hub name based on the provided properties.
    """
    properties[
        'EVENT_HUB'
    ] = f"topic-{properties['DOMAIN']}-{properties['datacontract']['workflow']['model']}"
    return properties


def get_container_name(properties: dict) -> str:
    """
    Gera e retorna o nome do container com base nas propriedades fornecidas.
    Generates and returns the container name based on the provided properties.
    """
    return f"ctrd{properties['DOMAIN']}raw"


def create_eventhub(
    eventhub_client: EventHubManagementClient,
    properties: dict,
    container_name: str,
) -> None:
    """
    Cria um Event Hub com as configurações especificadas.
    Creates an Event Hub with the specified configurations.
    """
    RESOURCE_GROUP_NAME = properties['RESOURCE_GROUP_NAME']
    STORAGE_ACCOUNT_NAME = properties['STORAGE_ACCOUNT_NAME']

    eventhub = eventhub_client.event_hubs.create_or_update(
        RESOURCE_GROUP_NAME,
        properties['EVENTHUB_NAMESPACE_NAME'],
        properties['EVENT_HUB'],
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
                    'archive_name_format': '{Namespace}/{EventHub}/{Year}/{Month}/{Day}/{PartitionId}/{Hour}_{Minute}_{Second}',
                },
            },
        },
    )
    log_info('Create EventHub: {}'.format(eventhub))


def get_or_create_eventhub(
    eventhub_client: EventHubManagementClient,
    properties: dict,
) -> None:
    """
    Tenta obter o Event Hub, se não existir, cria um novo.
    Tries to get the Event Hub, if it doesn't exist, creates a new one.
    """
    RESOURCE_GROUP_NAME = properties['RESOURCE_GROUP_NAME']
    EVENTHUB_NAMESPACE_NAME = properties['EVENTHUB_NAMESPACE_NAME']

    try:
        # Tenta obter o Event Hub
        # Tries to get the Event Hub
        eventhub = eventhub_client.event_hubs.get(
            RESOURCE_GROUP_NAME,
            EVENTHUB_NAMESPACE_NAME,
            properties['EVENT_HUB'],
        )
        log_info('Topico Event Hub já existe: {}'.format(eventhub))
    except ResourceNotFoundError:
        # Cria o Event Hub se não existir
        # Creates the Event Hub if it doesn't exist
        create_eventhub(
            eventhub_client,
            properties,
            get_container_name(properties),
        )
        # Obtém o Event Hub criado
        # Gets the created Event Hub
        eventhub = eventhub_client.event_hubs.get(
            RESOURCE_GROUP_NAME,
            EVENTHUB_NAMESPACE_NAME,
            properties['EVENT_HUB'],
        )
        log_info('Get EventHub: {}'.format(eventhub))


def create_event_hub_ingest(properties: dict) -> None:
    """
    Função principal para criar ou obter um Event Hub de ingestão com base nas propriedades fornecidas.
    Main function to create or get an ingestion Event Hub based on the provided properties.

    Parâmetros:
    - properties (dict): Dicionário contendo as propriedades necessárias.
    Parameters:
    - properties (dict): Dictionary containing the necessary properties.
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
    properties = get_eventhub_name(properties)
    get_or_create_eventhub(eventhub_client, properties)
