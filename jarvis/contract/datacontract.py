from azure_objects.event_hub import create_event_hub_ingest
from databricks_objects.workflow import create_job_ingest
from utils.logger import log_error, log_info

def get_value(properties: dict, keys: list, force_error: bool = False) -> str:
    """
    Função para obter o valor de uma chave aninhada em um dicionário.
    Function to get the value of a nested key in a dictionary.

    Parâmetros:
    - properties (dict): O dicionário de onde o valor será obtido.
    - keys (list): Uma lista de chaves que representam o caminho até o valor desejado.
    - force_error (bool): Se True, lança um erro se a chave não for encontrada.
    Parameters:
    - properties (dict): The dictionary from which the value will be obtained.
    - keys (list): A list of keys representing the path to the desired value.
    - force_error (bool): If True, raises an error if the key is not found.

    Retorno:
    - str: O valor da chave se existir, caso contrário, uma string vazia.
    Returns:
    - str: The value of the key if it exists, otherwise an empty string.
    """
    try:
        value = properties
        for key in keys:
            if key in value:
                value = value[key]
            else:
                raise KeyError(f"Key '{key}' not found in properties.")
        value = '' if value is None else value
        return value
    except KeyError as e:
        if force_error:
            raise log_error(f'KeyError: {e}')
        return ''
    except Exception as e:
        raise log_error(f'An unexpected error occurred: {e}')

def get_domain(properties: dict) -> dict:
    """
    Função para retornar o domínio do primeiro modelo encontrado.
    Function to return the domain of the first found model.

    Parâmetros:
    - properties (dict): O dicionário de onde o domínio será obtido.
    Parameters:
    - properties (dict): The dictionary from which the domain will be obtained.

    Retorno:
    - dict: O dicionário original com o domínio adicionado.
    Returns:
    - dict: The original dictionary with the domain added.
    """
    try:
        return {
            **properties,
            'DOMAIN': properties['datacontract']['info']['domain'],
        }
    except (KeyError, StopIteration) as e:
        raise log_error(f'Error: {e}')
        return None

def create_event_hub(properties: dict):
    """
    Função para criar um Event Hub.
    Function to create an Event Hub.

    Parâmetros:
    - properties (dict): O dicionário de propriedades necessário para criar o Event Hub.
    Parameters:
    - properties (dict): The dictionary of properties needed to create the Event Hub.
    """
    log_info('Creating Event Hub...')
    create_event_hub_ingest(properties)
    log_info('Event Hub created successfully.')

def create_databricks_workflow(properties: dict):
    """
    Função para criar um workflow no Databricks.
    Function to create a workflow in Databricks.

    Parâmetros:
    - properties (dict): O dicionário de propriedades necessário para criar o workflow.
    Parameters:
    - properties (dict): The dictionary of properties needed to create the workflow.
    """
    log_info('Creating Workflow Databricks...')
    create_job_ingest(properties)
    log_info('Workflow Databricks created successfully.')

def datacontract_ingest_create_workflow(properties: dict):
    """
    Função para criar workflows de ingestão com base nas propriedades fornecidas.
    Function to create ingestion workflows based on the provided properties.

    Parâmetros:
    - properties (dict): O dicionário de propriedades necessário para criar os workflows.
    Parameters:
    - properties (dict): The dictionary of properties needed to create the workflows.
    """
    try:
        if properties['datacontract']['ingest_workflow']['source']['type'] == 'eventhub':
            create_event_hub(properties)

        if properties['datacontract']['servers']['development']['type'] == 'databricks':
            create_databricks_workflow(properties)
        else:
            raise ValueError(
                f"Error: Invalid source type '{properties['datacontract']['ingest_workflow']['source']['type']}'"
            )
    except KeyError as e:
        raise log_error(f'Key error: {e}')
    except ValueError as e:
        raise log_error(e)
    except Exception as e:
        raise log_error(f'An unexpected error occurred: {e}')