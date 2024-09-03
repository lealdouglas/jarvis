from contract.datacontract import get_value


def define_job_parameters(properties: dict[str, any]) -> dict[str, any]:

    src_path = ''
    if (
        properties['datacontract']['ingest_workflow']['source']['type']
        == 'eventhub'
    ):
        src_path = f"/{properties['EVENTHUB_NAMESPACE_NAME']}/topic-{properties['DOMAIN']}-{properties['datacontract']['ingest_workflow']['model']}"
    else:
        src_path = f"{get_value(properties, ['datacontract','ingest_workflow','model'], True)}"

    default_lst = [
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
        f"{get_value(properties, ['datacontract','ingest_workflow','model'], True)}"
    ]

    return {
        **properties,
        'CARLTON_SOURCE_PARAMETERS': [
            *default_lst,
            '-file_extension',
            get_value(
                properties,
                ['datacontract', 'ingest_workflow', 'source', 'format'],
                True,
            ),
            '-path_src',
            src_path,
            '-file_header',
            f"{get_value(properties, ['datacontract','ingest_workflow','source','header'])}",
            '-file_delimiter',
            f"{get_value(properties, ['datacontract','ingest_workflow','source','delimiter'])}",
        ],
    }
