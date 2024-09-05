from contract.datacontract import get_value

from contract.datacontract import get_value


def get_source_path(properties: dict[str, any]) -> str:
    """
    Retorna o caminho da fonte com base nas propriedades fornecidas.
    """
    if (
        properties['datacontract']['ingest_workflow']['source']['type']
        == 'eventhub'
    ):
        return f"/{properties['EVENTHUB_NAMESPACE_NAME']}/topic-{properties['DOMAIN']}-{properties['datacontract']['ingest_workflow']['model']}"
    return get_value(
        properties, ['datacontract', 'ingest_workflow', 'model'], True
    )


def get_default_parameters(properties: dict[str, any]) -> list[str]:
    """
    Retorna a lista de parâmetros padrão para a ingestão.
    """
    return [
        '-storage_name_src',
        properties['STORAGE_ACCOUNT_NAME'],
        '-container_src',
        f"ctr{properties['DOMAIN']}raw",
        '-file_resource',
        'adls',
        '-type_run',
        properties['datacontract']['servicelevels']['frequency']['type'],
        '-storage_name_tgt',
        properties['STORAGE_ACCOUNT_NAME'],
        '-container_tgt',
        'dtmaster-catalog',
        '-schema_name',
        'bronze',
        '-table_name',
        get_value(
            properties, ['datacontract', 'ingest_workflow', 'model'], True
        ),
    ]


def get_carlton_source_parameters(
    properties: dict[str, any], src_path: str
) -> list[str]:
    """
    Retorna a lista de parâmetros específicos do Carlton Source.
    """
    return [
        '-file_extension',
        get_value(
            properties,
            ['datacontract', 'ingest_workflow', 'source', 'format'],
            True,
        ),
        '-path_src',
        src_path,
        '-file_header',
        get_value(
            properties, ['datacontract', 'ingest_workflow', 'source', 'header']
        ),
        '-file_delimiter',
        get_value(
            properties,
            ['datacontract', 'ingest_workflow', 'source', 'delimiter'],
        ),
    ]


def define_job_parameters(properties: dict[str, any]) -> dict[str, any]:
    """
    Define os parâmetros do trabalho de ingestão com base nas propriedades fornecidas.
    """
    src_path = get_source_path(properties)
    default_lst = get_default_parameters(properties)
    carlton_source_parameters = get_carlton_source_parameters(
        properties, src_path
    )

    return {
        **properties,
        'CARLTON_SOURCE_PARAMETERS': [
            *default_lst,
            *carlton_source_parameters,
        ],
    }
