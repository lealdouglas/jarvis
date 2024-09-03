from azure_objects.event_hub import create_event_hub_ingest
from databricks_objects.workflow import create_job_ingest
from utils.logger import log_error, log_info


def get_value(properties: dict, keys: list, force_error: bool = False) -> str:
    """
    Função para obter o valor de uma chave aninhada em um dicionário.

    Parâmetros:
    - properties (dict): O dicionário de onde o valor será obtido.
    - keys (list): Uma lista de chaves que representam o caminho até o valor desejado.

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
        value = '' if value == None else value
        return value
    except KeyError as e:
        if force_error == True:
            raise log_error(f'KeyError: {e}')
        return ''
    except Exception as e:
        raise log_error(f'An unexpected error occurred: {e}')


def datacontract_get_domain(properties: dict):
    """
    Função para retornar o domínio do primeiro modelo encontrado.

    Esta função realiza as seguintes operações:
    1. Acessa a chave 'models' no dicionário fornecido.
    2. Obtém o primeiro modelo encontrado dentro de 'models'.
    3. Retorna o valor do campo 'domain' do primeiro modelo, se existir.

    Exemplo de uso:
    ```python
    domain = get_domain(datacontract)
    ```

    Retorno:
    - O valor do campo 'domain' do primeiro modelo encontrado, ou None se não existir.
    """

    try:
        return {
            **properties,
            'DOMAIN': properties['datacontract']['info']['domain'],
        }

    except (KeyError, StopIteration) as e:
        raise log_error(f'Error: {e}')
        return None


def datacontract_ingest_create_workflow(properties: dict):
    try:
        # Validar se recurso.origem.nome_topico_event_hub é igual a 'nome_topico_event_hub'
        if (
            properties['datacontract']['ingest_workflow']['source']['type']
            == 'eventhub'
        ):
            log_info('Creating Event Hub...')
            create_event_hub_ingest(properties)
            log_info('Event Hub created successfully.')

        if (
            properties['datacontract']['servers']['development']['type']
            == 'databricks'
        ):
            log_info('Creating Workflow Databricks...')
            create_job_ingest(properties)
            log_info('Workflow Databricks created successfully.')
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
