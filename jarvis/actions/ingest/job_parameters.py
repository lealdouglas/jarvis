from jarvis.contract.datacontract import get_value


def get_source_path(properties: dict[str, any]) -> str:
    """
    Retorna o caminho da fonte com base nas propriedades fornecidas.
    Returns the source path based on the provided properties.
    """
    if properties['datacontract']['workflow']['source']['type'] == 'eventhub':
        return f"/{properties['EVENTHUB_NAMESPACE_NAME']}/topic-{properties['DOMAIN']}-{properties['datacontract']['workflow']['model']}"
    return get_value(properties, ['datacontract', 'workflow', 'model'], True)


def get_default_parameters(properties: dict[str, any]) -> list[str]:
    """
    Retorna a lista de parâmetros padrão para a ingestão.
    Returns the default parameters list for ingestion.
    """
    return [
        '-storage_name_src',
        properties['STORAGE_ACCOUNT_NAME'],
        '-container_src',
        f"ctrd{properties['DOMAIN']}raw",
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
        get_value(properties, ['datacontract', 'workflow', 'model'], True),
    ]


def get_carlton_source_parameters(
    properties: dict[str, any], src_path: str
) -> list[str]:
    """
    Retorna a lista de parâmetros específicos do Carlton Source.
    Returns the specific parameters list for Carlton Source.
    """
    return [
        '-file_extension',
        get_value(
            properties,
            ['datacontract', 'workflow', 'source', 'format'],
            True,
        ),
        '-path_src',
        src_path,
        '-file_header',
        get_value(
            properties, ['datacontract', 'workflow', 'source', 'header']
        ),
        '-file_delimiter',
        get_value(
            properties,
            ['datacontract', 'workflow', 'source', 'delimiter'],
        ),
    ]


def define_job_parameters(
    properties: dict[str, any], job_type='ingest'
) -> dict[str, any]:
    """
    Define os parâmetros do trabalho de ingestão com base nas propriedades fornecidas.
    Defines the ingestion job parameters based on the provided properties.
    """

    if job_type == 'prep':
        return {
            **properties,
            'table_prep': get_value(
                properties,
                ['datacontract', 'workflow', 'model'],
                True,
            ),
        }

    src_path = get_source_path(
        properties
    )  # Obtém o caminho da fonte / Gets the source path
    default_lst = get_default_parameters(
        properties
    )  # Obtém os parâmetros padrão / Gets the default parameters
    carlton_source_parameters = get_carlton_source_parameters(
        properties, src_path
    )  # Obtém os parâmetros específicos do Carlton Source / Gets the specific parameters for Carlton Source

    return {
        **properties,
        'CARLTON_SOURCE_PARAMETERS': [
            *default_lst,
            *carlton_source_parameters,
        ],
    }
