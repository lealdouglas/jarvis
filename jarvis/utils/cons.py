import os

# Configuracoes do service principal1
CLIENT_ID = os.environ.get('ARM_CLIENT_ID', 'Not Set')
TENANT_ID = os.environ.get('ARM_TENANT_ID', 'Not Set')
CLIENT_SECRET = os.environ.get('ARM_CLIENT_SECRET', 'Not Set')

# Configuracoes do ambiente azure
SUBSCRIPTION_ID = os.environ.get('ARM_SUBSCRIPTION_ID', 'Not Set')

# RESOURCE_GROUP_NAME = (os.environ.get('RESOURCE_GROUP_NAME', 'RESOURCE_GROUP_NAME'))
