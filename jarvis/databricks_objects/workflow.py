from databricks.sdk.service.compute import Library, PythonPyPiLibrary
from databricks.sdk.service.jobs import (
    CronSchedule,
    JobEmailNotifications,
    PauseStatus,
    PythonWheelTask,
    Task,
    TaskDependency,
)

from jarvis.databricks_objects.credential import work_credential
from jarvis.databricks_objects.find_whl import get_lasted_version_whl
from jarvis.utils.cons import SUBSCRIPTION_ID
from jarvis.utils.helper import validate_args
from jarvis.utils.logger import log_error, log_info


def validate_properties(properties: dict) -> None:
    """
    Valida se as propriedades necessárias estão presentes.
    """
    validate_args(['CLUSTER_ID', 'CARLTON_SOURCE_PARAMETERS'], properties)


def get_job_name(properties: dict, job_type='ingest') -> str:
    """
    Gera e retorna o nome do job com base nas propriedades fornecidas.
    """
    if job_type == 'ingest':
        return f"ingest-{properties['DOMAIN']}-{properties['datacontract']['workflow']['model']}"
    else:
        return f"prep-{properties['DOMAIN']}-{properties['datacontract']['workflow']['model']}"


def delete_existing_job(w, job_name: str) -> None:
    """
    Deleta o job existente se já existir.
    """
    try:
        for job in w.jobs.list():
            if job.settings.name == job_name:
                w.jobs.delete(job_id=job.job_id)
                log_info(f'Deleted existing job: {job_name}')
    except Exception as e:
        log_error(f'Error deleting existing job: {e}')
        raise


def create_mock_job(w, job_name: str, properties: dict) -> None:
    """
    Cria um novo job para simular eventos event hub.
    """
    try:
        job = w.jobs.create(
            name=f'mock-{job_name}',
            schedule=CronSchedule(
                quartz_cron_expression='*/5 * * * *',
                timezone_id='America/Sao_Paulo',
                pause_status=PauseStatus('PAUSED'),
            ),
            email_notifications=JobEmailNotifications(
                on_start=properties['datacontract']['workflow'][
                    'email_notifications'
                ]['on_start'],
                on_success=properties['datacontract']['workflow'][
                    'email_notifications'
                ]['on_success'],
                on_failure=properties['datacontract']['workflow'][
                    'email_notifications'
                ]['on_failure'],
            ),
            tasks=[
                Task(
                    description=f'job mock data mock-{job_name}',
                    python_wheel_task=PythonWheelTask(
                        entry_point='carlton',
                        package_name='carlton',
                        parameters=[
                            '-function',
                            'mock_data',
                            '-event_hub_namespace',
                            properties['EVENTHUB_NAMESPACE_NAME'],
                            '-event_hub_name',
                            properties['EVENT_HUB'],
                        ],
                    ),
                    task_key=f'task-mock-{job_name}',
                    existing_cluster_id=properties['CLUSTER_ID'],
                    libraries=[
                        Library(pypi=PythonPyPiLibrary(package='carlton'))
                    ],
                )
            ],
        )
        log_info(f'Job created successfully: {job_name}')
        log_info(f'View the job at {w.config.host}/#job/{job.job_id}\n')
    except Exception as e:
        log_error(f'Error creating job: {e}')
        raise


def create_new_job(w, job_name: str, properties: dict) -> None:
    """
    Cria um novo job de ingestão no Databricks.
    """
    try:
        job = w.jobs.create(
            name=job_name,
            schedule=CronSchedule(
                quartz_cron_expression=properties['datacontract'][
                    'servicelevels'
                ]['frequency']['cron'],
                timezone_id='America/Sao_Paulo',
                pause_status=PauseStatus('PAUSED'),
            ),
            email_notifications=JobEmailNotifications(
                on_start=properties['datacontract']['workflow'][
                    'email_notifications'
                ]['on_start'],
                on_success=properties['datacontract']['workflow'][
                    'email_notifications'
                ]['on_success'],
                on_failure=properties['datacontract']['workflow'][
                    'email_notifications'
                ]['on_failure'],
            ),
            tasks=[
                Task(
                    description=f'job ingest {job_name}',
                    python_wheel_task=PythonWheelTask(
                        entry_point='carlton',
                        package_name='carlton',
                        parameters=[
                            '-function',
                            'ingest',
                            *properties['CARLTON_SOURCE_PARAMETERS'],
                        ],
                    ),
                    task_key=f'task-{job_name}',
                    existing_cluster_id=properties['CLUSTER_ID'],
                    libraries=[
                        Library(pypi=PythonPyPiLibrary(package='carlton'))
                    ],
                )
            ],
        )
        log_info(f'Job created successfully: {job_name}')
        log_info(f'View the job at {w.config.host}/#job/{job.job_id}\n')
    except Exception as e:
        log_error(f'Error creating job: {e}')
        raise


def create_new_job_prep(w, job_name: str, properties: dict) -> None:
    """
    Cria um novo job de preparacao no Databricks.
    """
    try:
        job = w.jobs.create(
            name=job_name,
            schedule=CronSchedule(
                quartz_cron_expression=properties['datacontract'][
                    'servicelevels'
                ]['frequency']['cron'],
                timezone_id='America/Sao_Paulo',
                pause_status=PauseStatus('PAUSED'),
            ),
            email_notifications=JobEmailNotifications(
                on_start=properties['datacontract']['workflow'][
                    'email_notifications'
                ]['on_start'],
                on_success=properties['datacontract']['workflow'][
                    'email_notifications'
                ]['on_success'],
                on_failure=properties['datacontract']['workflow'][
                    'email_notifications'
                ]['on_failure'],
            ),
            tasks=[
                Task(
                    description=f'job create_table {job_name}',
                    python_wheel_task=PythonWheelTask(
                        entry_point='carlton',
                        package_name='carlton',
                        parameters=[
                            '-function',
                            'create_table',
                            '-table_name',
                            properties['table_prep'],
                        ],
                    ),
                    task_key='task-create-table-contract',
                    existing_cluster_id=properties['CLUSTER_ID'],
                    libraries=[
                        Library(pypi=PythonPyPiLibrary(package='carlton'))
                    ],
                ),
                Task(
                    description=f'job prep {job_name}',
                    depends_on=[
                        TaskDependency(task_key='task-create-table-contract')
                    ],
                    python_wheel_task=PythonWheelTask(
                        entry_point='main',
                        package_name='definition_project',
                    ),
                    task_key=f'task-{job_name}',
                    existing_cluster_id=properties['CLUSTER_ID'],
                    libraries=[
                        Library(
                            whl=get_lasted_version_whl(
                                w, '/Workspace/jarvis/prep/definition_project/'
                            )
                        )
                    ],
                ),
            ],
        )
        log_info(f'Job created successfully: {job_name}')
        log_info(f'View the job at {w.config.host}/#job/{job.job_id}\n')
    except Exception as e:
        log_error(f'Error creating job: {e}')
        raise


def create_job_ingest(properties: dict[str, str]) -> None:
    """
    Função principal para criar um job de ingestão no Databricks com base nas propriedades fornecidas.

    Parâmetros:
    - properties (dict): Dicionário contendo as propriedades necessárias.
    """
    try:
        validate_properties(properties)
        job_name = get_job_name(properties)
        w = work_credential()

        delete_existing_job(w, job_name)

        if (
            properties['datacontract']['workflow']['source']['job_mock']
            == True & properties['datacontract']['workflow']['source']['type']
            == 'eventhub'
        ):
            create_mock_job(w, job_name, properties)

        create_new_job(w, job_name, properties)

    except KeyError as e:
        raise log_error(f'Key error: {e}')
    except ValueError as e:
        raise log_error(f'Value error: {e}')
    except Exception as e:
        raise log_error(f'An unexpected error occurred: {e}')


def create_job_prep(properties: dict[str, str]) -> None:
    """
    Função principal para criar um job de ingestão no Databricks com base nas propriedades fornecidas.

    Parâmetros:
    - properties (dict): Dicionário contendo as propriedades necessárias.
    """
    try:
        job_name = get_job_name(properties, 'prep')
        w = work_credential()

        delete_existing_job(w, job_name)
        create_new_job_prep(w, job_name, properties)

    except KeyError as e:
        raise log_error(f'Key error: {e}')
    except ValueError as e:
        raise log_error(f'Value error: {e}')
    except Exception as e:
        raise log_error(f'An unexpected error occurred: {e}')
