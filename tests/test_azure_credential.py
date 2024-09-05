import pytest
from unittest.mock import patch, MagicMock
from azure.identity import ClientSecretCredential
from jarvis.databricks_objects.credential import auth_credential


@patch('jarvis.databricks_objects.credential.TENANT_ID', 'test-tenant-id')
@patch('jarvis.databricks_objects.credential.CLIENT_ID', 'test-client-id')
@patch(
    'jarvis.databricks_objects.credential.CLIENT_SECRET', 'test-client-secret'
)
def test_auth_credential():
    with patch(
        'azure.identity.ClientSecretCredential'
    ) as MockClientSecretCredential:
        mock_credential_instance = MagicMock(spec=ClientSecretCredential)
        MockClientSecretCredential.return_value = mock_credential_instance

        credential = auth_credential()

        MockClientSecretCredential.assert_called_once_with(
            tenant_id='test-tenant-id',
            client_id='test-client-id',
            client_secret='test-client-secret',
        )
        assert credential == mock_credential_instance
