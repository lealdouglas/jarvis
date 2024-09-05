import pytest
from unittest.mock import patch

# Importar as constantes do arquivo cons.py
from cons import (
    CLIENT_ID,
    TENANT_ID,
    CLIENT_SECRET,
    SUBSCRIPTION_ID,
    RESOURCE_GROUP_NAME,
)


def test_constants():
    assert CLIENT_ID == 'a08dc7fd-4c85-45f6-a23d-4c3f3ddba5a3'
    assert TENANT_ID == 'feacc2ad-e79f-4487-a551-40533842f77b'
    assert CLIENT_SECRET == '2de8Q~aqZ2vF_NTYBLWpbB6Ntzxu9O1hqw20Tc6q'
    assert SUBSCRIPTION_ID == '8a66b4be-4d16-49bb-9c92-7610ca4ac552'
    assert RESOURCE_GROUP_NAME == 'rsgstrifedtm'
