import os
import pytest
from unittest.mock import patch, MagicMock
from databricks.sdk import WorkspaceClient
from jarvis.databricks_objects.credential import work_credential

@patch.dict(os.environ, {'HOST': 'https://test-host'})
@patch('jarvis.databricks_objects.credential.CLIENT_ID', 'test-client-id')
@patch('jarvis.databricks_objects.credential.CLIENT_SECRET', 'test-client-secret')
@patch('jarvis.databricks_objects.credential.TENANT_ID', 'test-tenant-id')
def test_work_credential():
    with patch('databricks.sdk.WorkspaceClient') as MockWorkspaceClient:
        mock_client_instance = MagicMock(spec=WorkspaceClient)
        MockWorkspaceClient.return_value = mock_client_instance

        client = work_credential()

        MockWorkspaceClient.assert_called_once_with(
            host='https://test-host',
            azure_client_id='test-client-id',
            azure_client_secret='test-client-secret',
            azure_tenant_id='test-tenant-id'
        )
        assert client == mock_client_instance