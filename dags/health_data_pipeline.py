from airflow import DAG
from airflow.utils.task_group import TaskGroup
from pendulum import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType
from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import RenderConfig

import os
import sys
import time

sys.path.append('/usr/local/airflow/include/scripts')

from icd_extraction import extract_icd_data
from icd_json_csv import json_to_csv
from google_files_extraction import download_google_files
from sqlite_extraction import export_sqltables_to_csv
from inpatient_outpatient import inpatient_and_outpatient_files
from hospital_info_clean import hospitalinfo_cleaned

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def delay_task():
    time.sleep(20)  # Delay for 20 seconds

with DAG(
    'health_etl_data',
    default_args=default_args,
    description='A DAG to extract, transform, upload, and process healthcare data',
    schedule_interval='@monthly',
    catchup=False,
) as dag:

    # Task: Check if files and config file exist
    check_files = BashOperator(
        task_id='check_files',
        bash_command='ls -l /usr/local/airflow/include/dataset && ls -l /usr/local/airflow/include/scripts/config.txt'
    )

    # Task Group 1: Data Extraction
    with TaskGroup('data_extraction') as data_extraction:
        download_google_files_task = PythonOperator(
            task_id='download_google_files',
            python_callable=download_google_files,
        )
        export_sqltables_to_csv_task = PythonOperator(
            task_id='export_sqltables_to_csv',
            python_callable=export_sqltables_to_csv,
        )
        extract_icd_data_task = PythonOperator(
            task_id='extract_icd_data',
            python_callable=lambda: extract_icd_data(output_file='/usr/local/airflow/include/sources/icd_data.json'),
        )

        # Set dependencies within data_extraction group
        download_google_files_task >> export_sqltables_to_csv_task >> extract_icd_data_task

    # Task to add a delay after data extraction
    delay_after_extraction = PythonOperator(
        task_id='delay_after_extraction',
        python_callable=delay_task
    )

    # Task Group 2: Transformation
    with TaskGroup('transformation') as transformation:
        transform_json_to_csv_task = PythonOperator(
            task_id='transform_json_to_csv',
            python_callable=lambda: [
                json_to_csv(os.path.join('/usr/local/airflow/include/sources', 'icd_data.json'),
                            '/usr/local/airflow/include/dataset/icd_raw_data.csv'),
                json_to_csv(os.path.join('/usr/local/airflow/include/sources', 'inpatient_2011.json'),
                            '/usr/local/airflow/include/dataset/inpatient_2011.csv'),
                json_to_csv(os.path.join('/usr/local/airflow/include/sources', 'inpatient_2012.json'),
                            '/usr/local/airflow/include/dataset/inpatient_2012.csv'),
                json_to_csv(os.path.join('/usr/local/airflow/include/sources', 'inpatient_2013.json'),
                            '/usr/local/airflow/include/dataset/inpatient_2013.csv')
            ]
        )
        process_inpatient_outpatient_files_task = PythonOperator(
            task_id='process_inpatient_outpatient_files',
            python_callable=inpatient_and_outpatient_files,
            op_kwargs={'config_file': '/usr/local/airflow/include/scripts/config.txt'},
        )
        
        hospitalinfo_clean_task = PythonOperator(
            task_id='hospitalinfo_clean_task',
            python_callable=hospitalinfo_cleaned,
            op_kwargs={'config_file': '/usr/local/airflow/include/scripts/config.txt'}, 
        )

        # Set dependencies within transformation group
        transform_json_to_csv_task >> process_inpatient_outpatient_files_task >> hospitalinfo_clean_task

    # Task to add a delay after transformation
    delay_after_transformation = PythonOperator(
        task_id='delay_after_transformation',
        python_callable=delay_task
    )

    # Task Group 3: Upload to GCS
    with TaskGroup('upload_to_gcs') as upload_to_gcs:
        local_csv_path = '/usr/local/airflow/include/dataset/'
        gcs_destination_path = 'raw/'
        csv_files = [f for f in os.listdir(local_csv_path) if f.endswith('.csv')]

        upload_tasks = [
            LocalFilesystemToGCSOperator(
                task_id=f'upload_{csv_file.replace(".", "_")}',  
                src=os.path.join(local_csv_path, csv_file),
                dst=os.path.join(gcs_destination_path, csv_file),
                gcp_conn_id='gcp',  
                bucket='healthcare_data_pipeline',  
                mime_type='text/csv',
            )
            for csv_file in csv_files
        ]

    # Task to add a delay after upload
    delay_after_upload = PythonOperator(
        task_id='delay_after_upload',
        python_callable=delay_task
    )

    # Task Group 4: BigQuery and DBT Processing
    with TaskGroup('bigquery_and_dbt_processing') as bigquery_and_dbt_processing:
        
        # Task to create the 'healthcare_raw' dataset
        create_healthcare_raw_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id='create_healthcare_raw_dataset',
            dataset_id='healthcare_raw',
            gcp_conn_id='gcp',
        )

        # Task to create the 'healthcare_transformed' dataset
        create_healthcare_transformed_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id='create_healthcare_transformed_dataset',
            dataset_id='healthcare_transformed',
            gcp_conn_id='gcp',
        )

        # Set dependencies 
        create_healthcare_raw_dataset >> create_healthcare_transformed_dataset

        gcs_to_raw_tasks = [
            aql.load_file(
                task_id=f'gcs_to_{table_name}',  
                input_file=File(
                    f'gs://healthcare_data_pipeline/raw/{csv_file}',
                    conn_id='gcp',
                    filetype=FileType.CSV,
                ),
                output_table=Table(
                    name=table_name,
                    conn_id='gcp',
                    metadata=Metadata(schema='healthcare_raw')
                ),
                use_native_support=False,
            )
            for csv_file, table_name in [
                ('icd_raw_data.csv', 'raw_icd_data'),
                ('hospital_general_info.csv', 'raw_hospital_general_info'),
                ('inpatient_2011.csv', 'raw_inpatient_2011'),
                ('inpatient_2012.csv', 'raw_inpatient_2012'),
                ('inpatient_2013.csv', 'raw_inpatient_2013'),
                ('outpatient_charges_2011.csv', 'raw_outpatient_charges_2011'),
                ('outpatient_charges_2012.csv', 'raw_outpatient_charges_2012'),
                ('outpatient_charges_2013.csv', 'raw_outpatient_charges_2013'),
            ]
        ]

        # Add delay task between BigQuery load and DBT transform
        delay_between_bigquery_and_dbt = PythonOperator(
            task_id='delay_between_bigquery_and_dbt',
            python_callable=delay_task
        )

        transform = DbtTaskGroup(
            group_id='transform',
            project_config=DBT_PROJECT_CONFIG,
            profile_config=DBT_CONFIG,
            render_config=RenderConfig(
                load_method=LoadMode.DBT_LS,
                select=['path:models/transform']
            )
        )

        # Set dependencies in the BigQuery and DBT Processing group
        create_healthcare_transformed_dataset >> gcs_to_raw_tasks >> delay_between_bigquery_and_dbt >> transform

    # Define overall task dependencies
    check_files >> data_extraction >> delay_after_extraction >> transformation >> delay_after_transformation >> upload_to_gcs >> delay_after_upload >> bigquery_and_dbt_processing
