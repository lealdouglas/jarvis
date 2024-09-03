from databricks.sdk.service.jobs import (
    CronSchedule,
    NotebookTask,
    PauseStatus,
    PythonWheelTask,
    JobEmailNotifications,
    Source,
    Task,
)
from databricks_objects.credential import work_credential
from utils.cons import *

import os

from azure_objects.credential import auth_credential
from azure.mgmt.eventhub import EventHubManagementClient
from azure.core.exceptions import ResourceNotFoundError
from utils.helper import validate_args
from utils.cons import SUBSCRIPTION_ID
from utils.logger import log_info, log_error
from databricks.sdk.service.compute import PythonPyPiLibrary, Library


def create_job_ingest(properties: dict[str, str]) -> None:
    """
    Este módulo contém a função `create_job_ingest`, que é responsável por criar um job de ingestão no Databricks.

    Objetivo:
    ---------
    A função `create_job_ingest` tem como objetivo criar um job de ingestão no Databricks com base nas propriedades fornecidas. Ela realiza as seguintes operações principais:

    1. **Validação de Argumentos**:
    - Utiliza a função `validate_args` para garantir que os argumentos necessários estão presentes nas propriedades fornecidas.

    2. **Deletar Job Existente**:
    - Verifica se já existe um job com o mesmo nome e, se existir, deleta o job existente para evitar duplicações.

    3. **Criar Novo Job**:
    - Cria um novo job de ingestão no Databricks com as configurações especificadas, incluindo agendamento, notificações por e-mail e tarefas.

    Funções Utilizadas:
    -------------------
    - `validate_args`: Valida se os argumentos necessários estão presentes nas propriedades fornecidas.
    - `work_credential`: Obtém as credenciais de trabalho para interagir com a API do Databricks.
    - `log_info`: Registra mensagens informativas.
    - `log_error`: Registra mensagens de erro.

    Exceções Tratadas:
    ------------------
    - `KeyError`: Captura erros de chave ausente nas propriedades fornecidas.
    - `ValueError`: Captura erros de valor inválido nas propriedades fornecidas.
    - `Exception`: Captura qualquer outra exceção inesperada e registra uma mensagem de erro.

    Exemplo de Uso:
    ---------------
    ```python
    properties = {
        'CLUSTER_ID': 'cluster-id',
        'CARLTON_SOURCE_PARAMETERS': ['param1', 'param2'],
        'DOMAIN': 'example-domain',
        'datacontract': {
            'ingest_workflow': {
                'model': 'example-model',
                'email_notifications': {
                    'on_start': ['email@example.com'],
                    'on_success': ['email@example.com'],
                    'on_failure': ['email@example.com']
                }
            },
            'servicelevels': {
                'frequency': {
                    'cron': '0 0 * * *'
                }
            }
        }
    }

    create_job_ingest(properties)
    """
    try:
        validate_args(
            ['CLUSTER_ID', 'CARLTON_SOURCE_PARAMETERS'],
            properties,
        )

        JOB_NAME = f"ingest-{properties['DOMAIN']}-{properties['datacontract']['ingest_workflow']['model']}"

        w = work_credential()

        # Deletar o job existente se já existir
        try:
            for job in w.jobs.list():
                if job.settings.name == JOB_NAME:
                    w.jobs.delete(job_id=job.job_id)
                    log_info(f'Deleted existing job: {JOB_NAME}')
        except Exception as e:
            log_error(f'Error deleting existing job: {e}')
            raise

        # Criar um novo job
        try:
            j = w.jobs.create(
                name=JOB_NAME,
                schedule=CronSchedule(
                    quartz_cron_expression=properties['datacontract'][
                        'servicelevels'
                    ]['frequency']['cron'],
                    timezone_id='America/Sao_Paulo',
                    pause_status=PauseStatus('PAUSED'),
                ),
                email_notifications=JobEmailNotifications(
                    on_start=properties['datacontract']['ingest_workflow'][
                        'email_notifications'
                    ]['on_start'],
                    on_success=properties['datacontract']['ingest_workflow'][
                        'email_notifications'
                    ]['on_success'],
                    on_failure=properties['datacontract']['ingest_workflow'][
                        'email_notifications'
                    ]['on_failure'],
                ),
                tasks=[
                    Task(
                        description=f'job ingest {JOB_NAME}',
                        python_wheel_task=PythonWheelTask(
                            entry_point='carlton',
                            package_name='carlton',
                            parameters=[
                                '-function',
                                'ingest',
                                *properties['CARLTON_SOURCE_PARAMETERS'],
                            ],
                        ),
                        task_key=f'task-{JOB_NAME}',
                        existing_cluster_id=properties['CLUSTER_ID'],
                        libraries=[
                            Library(pypi=PythonPyPiLibrary(package='carlton'))
                        ],
                    )
                ],
            )
            log_info(f'Job created successfully: {JOB_NAME}')
            log_info(f'View the job at {w.config.host}/#job/{j.job_id}\n')
        except Exception as e:
            log_error(f'Error creating job: {e}')
            raise

    except KeyError as e:
        raise log_error(f'Key error: {e}')
    except ValueError as e:
        raise log_error(f'Value error: {e}')
    except Exception as e:
        raise log_error(f'An unexpected error occurred: {e}')
