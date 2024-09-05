from azure_objects.event_hub import create_event_hub_ingest
from databricks_objects.workflow import create_job_ingest
from utils.logger import log_error, log_info


def get_value(properties: dict, keys: list, force_error: bool = False) -> str:
    """
    Função para obter o valor de uma chave aninhada em um dicionário.

    Parâmetros:
    - properties (dict): O dicionário de onde o valor será obtido.
    - keys (list): Uma lista de chaves que representam o caminho até o valor desejado.
    - force_error (bool): Se True, lança um erro se a chave não for encontrada.

    Retorno:
    - str: O valor da chave se existir, caso contrário, uma string vazia.
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

    Parâmetros:
    - properties (dict): O dicionário de onde o domínio será obtido.

    Retorno:
    - dict: O dicionário original com o domínio adicionado.
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

    Parâmetros:
    - properties (dict): O dicionário de propriedades necessário para criar o Event Hub.
    """
    log_info('Creating Event Hub...')
    create_event_hub_ingest(properties)
    log_info('Event Hub created successfully.')


def create_databricks_workflow(properties: dict):
    """
    Função para criar um workflow no Databricks.

    Parâmetros:
    - properties (dict): O dicionário de propriedades necessário para criar o workflow.
    """
    log_info('Creating Workflow Databricks...')
    create_job_ingest(properties)
    log_info('Workflow Databricks created successfully.')


def datacontract_ingest_create_workflow(properties: dict):
    """
    Função para criar workflows de ingestão com base nas propriedades fornecidas.

    Parâmetros:
    - properties (dict): O dicionário de propriedades necessário para criar os workflows.
    """
    try:
        if (
            properties['datacontract']['ingest_workflow']['source']['type']
            == 'eventhub'
        ):
            create_event_hub(properties)
        elif (
            properties['datacontract']['servers']['development']['type']
            == 'databricks'
        ):
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
