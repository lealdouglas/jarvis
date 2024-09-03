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
from utils.logger import log_info


def create_event_hub_ingest(properties: dict) -> None:

    validate_args(
        [
            'RESOURCE_GROUP_NAME',
            'STORAGE_ACCOUNT_NAME',
            'EVENTHUB_NAMESPACE_NAME',
            'DOMAIN',
        ],
        properties,
    )

    RESOURCE_GROUP_NAME = properties['RESOURCE_GROUP_NAME']
    STORAGE_ACCOUNT_NAME = properties['STORAGE_ACCOUNT_NAME']
    EVENTHUB_NAMESPACE_NAME = properties['EVENTHUB_NAMESPACE_NAME']
    EVENTHUB_NAME = f"topic-{properties['DOMAIN']}-{properties['datacontract']['ingest_workflow']['model']}"
    CONTAINER_NAME = f"ctr{properties['DOMAIN']}raw"

    eventhub_client = EventHubManagementClient(
        credential=auth_credential(), subscription_id=SUBSCRIPTION_ID
    )

    try:
        # Tenta obter o Event Hub
        eventhub = eventhub_client.event_hubs.get(
            RESOURCE_GROUP_NAME, EVENTHUB_NAMESPACE_NAME, EVENTHUB_NAME
        )
        log_info('Topico Event Hub já existe: {}'.format(eventhub))

    except ResourceNotFoundError:

        # Create EventHub
        eventhub = eventhub_client.event_hubs.create_or_update(
            RESOURCE_GROUP_NAME,
            EVENTHUB_NAMESPACE_NAME,
            EVENTHUB_NAME,
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
                        'storage_account_resource_id': '/subscriptions/'
                        + SUBSCRIPTION_ID
                        + '/resourceGroups/'
                        + RESOURCE_GROUP_NAME
                        + '/providers/Microsoft.Storage/storageAccounts/'
                        + STORAGE_ACCOUNT_NAME
                        + '',
                        'blob_container': CONTAINER_NAME,
                        'archive_name_format': '{Namespace}/{EventHub}/{PartitionId}/{Year}/{Month}/{Day}/{Hour}/{Minute}/{Second}',
                    },
                },
            },
        )
        log_info('Create EventHub: {}'.format(eventhub))

        # Get EventHub
        eventhub = eventhub_client.event_hubs.get(
            RESOURCE_GROUP_NAME, EVENTHUB_NAMESPACE_NAME, EVENTHUB_NAME
        )
        log_info('Get EventHub: {}'.format(eventhub))
