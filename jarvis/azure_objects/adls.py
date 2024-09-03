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


def create_container_ingest(properties: dict) -> None:

    validate_args(
        [
            'RESOURCE_GROUP_NAME',
            'STORAGE_ACCOUNT_NAME',
            'DOMAIN',
        ],
        properties,
    )

    RESOURCE_GROUP_NAME = properties['RESOURCE_GROUP_NAME']
    STORAGE_ACCOUNT_NAME = properties['STORAGE_ACCOUNT_NAME']
    CONTAINER_NAME = f"ctr{properties['DOMAIN']}raw"

    return {
        **properties,
        'CARLTON_SOURCE_PARAMETERS': [
            '-storage_name_src',
            STORAGE_ACCOUNT_NAME,
            '-container_src',
            CONTAINER_NAME,
            '-path_src',
            f"/{properties['datacontract']['ingest_workflow']['model']}",
            '-file_resource',
            'adls',
            '-type_run',
            properties['datacontract']['servicelevels']['frequency']['type'],
            '-file_extension',
            properties['datacontract']['ingest_workflow']['source']['format'],
            '-storage_name_tgt',
            STORAGE_ACCOUNT_NAME,
            '-container_tgt',
            'dtmaster-catalog',
        ],
    }
