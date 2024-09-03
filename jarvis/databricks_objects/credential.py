import os
import sys

from databricks.sdk import WorkspaceClient

from utils.cons import *


def work_credential() -> WorkspaceClient:

    w = WorkspaceClient(
        host=os.environ.get('HOST', 'Not Set'),
        azure_client_id=CLIENT_ID,
        azure_client_secret=CLIENT_SECRET,
        azure_tenant_id=TENANT_ID,
    )

    return w
