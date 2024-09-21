from databricks.sdk.service.jobs import (
    CronSchedule,
    JobEmailNotifications,
    PauseStatus,
    PythonWheelTask,
    Task,
)
from databricks_objects.credential import work_credential
from utils.cons import SUBSCRIPTION_ID
from utils.helper import validate_args
from utils.logger import log_info, log_error
from databricks.sdk.service.compute import PythonPyPiLibrary, Library


def validate_properties(properties: dict) -> None:
    """
    Valida se as propriedades necessárias estão presentes.
    """
    validate_args(['CLUSTER_ID', 'CARLTON_SOURCE_PARAMETERS'], properties)


def get_job_name(properties: dict) -> str:
    """
    Gera e retorna o nome do job com base nas propriedades fornecidas.
    """
    return f"ingest-{properties['DOMAIN']}-{properties['datacontract']['ingest_workflow']['model']}"


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
        create_new_job(w, job_name, properties)

    except KeyError as e:
        raise log_error(f'Key error: {e}')
    except ValueError as e:
        raise log_error(f'Value error: {e}')
    except Exception as e:
        raise log_error(f'An unexpected error occurred: {e}')
