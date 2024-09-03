from azure.identity import ClientSecretCredential
from utils.cons import *


def auth_credential() -> ClientSecretCredential:
    credential = ClientSecretCredential(
        tenant_id=TENANT_ID, client_id=CLIENT_ID, client_secret=CLIENT_SECRET
    )

    return credential
