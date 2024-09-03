import datetime
import json
import os

import pytz
import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import ClusterSpec
from databricks.sdk.service.jobs import (
    CronSchedule,
    NotebookTask,
    Source,
    Task,
)
from ingest_config import IngestConfig


def ingest():

    ingest_config = IngestConfig()

    host = 'https://adb-1751348342569497.17.azuredatabricks.net/'
    warehouse_id = 'a549ae80a7fc8fb9'
    catalog = 'dev_catalog'
    schema = 'bronze'
    cluster_id = '0511-142827-2b9pm5x8'
    notebook_path = '/dtmaster-jarvis/ingest_run'

    # # ============================================================================================================================

    # statement_id = w.statement_execution.execute_statement(
    #     warehouse_id=warehouse_id,
    #     catalog=catalog,
    #     schema=schema,
    #     statement="""
    #     CREATE TABLE IF NOT EXISTS information_ingest (
    #     table_catalog STRING, table_schema STRING, table_name STRING, table_path STRING, table_ext STRING, table_owner STRING, table_access_group STRING, date_time STRING)
    #     TBLPROPERTIES (delta.enableDeletionVectors=false)
    #     """,
    # ).statement_id

    # result = w.statement_execution.get_statement(statement_id)

    # print('CREATE TABLE IF NOT EXISTS information_ingest')
    # while 'SUCCEEDED' not in str(result.status):
    #     result = w.statement_execution.get_statement(statement_id)

    #     if 'FAILED' in str(result.status):
    #         print(result.status)
    #         break

    # # ============================================================================================================================

    # current_time = str(datetime.datetime.now(pytz.timezone('America/Sao_Paulo')).strftime('%d/%m/%Y %H:%M:%S'))

    # statement_id = w.statement_execution.execute_statement(
    #     warehouse_id=warehouse_id,
    #     catalog=catalog,
    #     schema=schema,
    #     statement=f"""     

    #     MERGE INTO information_ingest AS i
    #     USING (SELECT 
    #         '{catalog}' as table_catalog,
    #         '{schema}' as table_schema,
    #         '{ingest_config.table_name}' as table_name,
    #         '{ingest_config.table_path}' as table_path,
    #         '{ingest_config.table_ext}' as table_ext,
    #         '{ingest_config.table_owner}' as table_owner,
    #         '{ingest_config.table_access_group}' as table_access_group,
    #         '{current_time}' as date_time
    #         ) AS n
    #     ON i.table_name = n.table_name
    #     WHEN MATCHED THEN UPDATE SET *
    #     WHEN NOT MATCHED THEN INSERT *
    #     """,
    # ).statement_id

    # result = w.statement_execution.get_statement(statement_id)

    # print('MERGE INTO information_ingest')
    # while 'SUCCEEDED' not in str(result.status):
    #     result = w.statement_execution.get_statement(statement_id)

    #     if 'FAILED' in str(result.status):
    #         print(result.status)
    #         break

    # # ============================================================================================================================

    # statement_id = w.statement_execution.execute_statement(
    #     warehouse_id=warehouse_id,
    #     catalog=catalog,
    #     schema=schema,
    #     statement=f"""     
    #     ALTER TABLE information_ingest
    #     SET TAGS ('t':'t'))
    #     """,
    # ).statement_id

    # result = w.statement_execution.get_statement(statement_id)

    # print('SET OWNER information_ingest')
    # while 'SUCCEEDED' not in str(result.status):
    #     result = w.statement_execution.get_statement(statement_id)

    #     if 'FAILED' in str(result.status):
    #         print(result.status)
    #         break

    # # ============================================================================================================================

    # statement_id = w.statement_execution.execute_statement(
    #     warehouse_id=warehouse_id,
    #     catalog=catalog,
    #     schema=schema,
    #     statement=f"""     
    #     ALTER TABLE information_ingest
    #     SET OWNER TO data_engineer 
    #     """,
    # ).statement_id

    # result = w.statement_execution.get_statement(statement_id)

    # print('SET OWNER information_ingest')
    # while 'SUCCEEDED' not in str(result.status):
    #     result = w.statement_execution.get_statement(statement_id)

    #     if 'FAILED' in str(result.status):
    #         print(result.status)
    #         break

    # # ============================================================================================================================


    print("Attempting to create the job. Please wait...\n")

    j = w.jobs.create(
    name = f'{schema}_{ingest_config.table_name}',
    schedule = CronSchedule(
        quartz_cron_expression = ingest_config.job_calendario,
        timezone_id = 'America/Sao_Paulo',
    ),
    tasks = [
        Task(
        description = '',
        notebook_task = NotebookTask(
            base_parameters = dict(""),
            notebook_path = notebook_path,
            source = Source("WORKSPACE"),
        ),
        task_key = f'{schema}_{ingest_config.table_name}',
        existing_cluster_id=cluster_id,
        # new_cluster = ClusterSpec(
        #     spark_version = '14.3.x-scala2.12',
        #     num_workers=0,
        #     node_type_id='Standard_F4',
        #     driver_node_type_id='Standard_F4',
        #     spark_conf={
        #         "spark.master":"local[*, 4]",
        #         "spark.databricks.cluster.profile":"singleNode"},
        # ),
        )
    ]
    )

    print(f"View the job at {w.config.host}/#job/{j.job_id}\n")


ingest()
