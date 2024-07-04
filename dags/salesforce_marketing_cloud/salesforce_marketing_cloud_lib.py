"""
Functions used in salesforce marketing cloud pipeline
"""

# ======================================================================================================================
# Helper Functions
# ======================================================================================================================
def copy_bq_table(
    source_project, source_dataset, source_table_name, landing_project, landing_dataset, landing_table_name, credentials
):
    """
    function to copy tables in BQ
        Parameters:
            source_project (str):
            source_dataset (str):
            source_table_name (string):
            landing_project (str):
            landing_dataset (str):
            landing_table_name (string):
            credentials (obj):
        Returns:
            None
    """

    from google.cloud import bigquery

    # GCP credentials
    src_bigquery_client = bigquery.Client.from_service_account_json(json_credentials_path=credentials,project=source_project) # bigquery.Client(credentials=credentials, project=source_project)
    src_dataset_ref = src_bigquery_client.dataset(source_dataset)
    src_table_ref = src_dataset_ref.table(source_table_name)

    lnd_bigquery_client = bigquery.Client.from_service_account_json(json_credentials_path=credentials, project=landing_project) # bigquery.Client(credentials=credentials, project=landing_project)
    lnd_dataset_ref = lnd_bigquery_client.dataset(landing_dataset)
    lnd_table_ref = lnd_dataset_ref.table(landing_table_name)

    job_config = bigquery.CopyJobConfig()
    job_config.write_disposition = "WRITE_TRUNCATE"
    job = src_bigquery_client.copy_table(src_table_ref, lnd_table_ref, location="US", job_config=job_config)

    src_bigquery_client.close()
    lnd_bigquery_client.close()


def archive_tables(project_id, dataset, arc_project, arc_dataset, gcp_key_path, **context):
    """
    Archive table
    Uses helper function
        Parameters:
            project_id (str):
            dataset (str):
            arc_project (string):
            arc_dataset (string):
            gcp_key_path (obj):
        Returns:
            None
    """
    from datetime import date
    from datetime import timedelta
    from google.cloud import bigquery

    # GCP credentials
    bigquery_client = bigquery.Client.from_service_account_json(json_credentials_path=gcp_key_path, project=project_id) #bigquery.Client(credentials=credentials, project=project_id)

    yesterday = date.today() - timedelta(1)
    archive_pattern = yesterday.strftime('%Y%m%d')

    for table in bigquery_client.list_tables(dataset):
        # print(table.table_id)
        copy_bq_table(
            project_id,
            dataset,
            table.table_id,
            arc_project,
            arc_dataset,
            table.table_id + "_" + archive_pattern,
            gcp_key_path,
        )

    print("Files have been archived")
    bigquery_client.close()
# ======================================================================================================================
# Pipeline Functions
# ======================================================================================================================
def sfmc_s3_to_gcs(aws_source_bucket_name, aws_access_key_id, aws_secret_access_key, gcs_bucket, gcp_project, gcp_key_path, delete_aws_source_file):
    import sys, os
    sys.path.insert(1, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    from dataengineering.check_objects import check_objects
    from dataengineering.upload_to_gcs import cloud_to_gcs

    # move any detected parquet files from s3 bucket by passing a list of file_names
    de_kwargs = {
        'is_destination_table': True,
        'add_table_prefix': False,
        'add_table_suffix': False,
        'force_alphanumeric_together': False,
        'remove_file_prefix': False,
        'remove_file_trailing_delimiter': "",
        'file_prefix': "",
        'file_type_filter': "csv"
    }
    s3_objects = check_objects('s3', aws_source_bucket_name, aws_access_key_id=aws_access_key_id,
                               aws_secret_access_key=aws_secret_access_key, **de_kwargs)
    file_list = [d['source_object'] for d in s3_objects]

    # upload parquet files from s3 to gcs bucket
    for file in file_list:
        subfolder = file[0:len(file) - 13]
        cloud_to_gcs_args = {'gcp_key_path': gcp_key_path,
                             'source_type': 's3',
                             'source': aws_source_bucket_name,
                             'destination_project_id': gcp_project,
                             'destination': gcs_bucket,
                             'subfolder': subfolder,
                             'file': file,
                             'aws_access_key_id': aws_access_key_id,
                             'aws_secret_access_key': aws_secret_access_key,
                             'delete_source': delete_aws_source_file,
                             'overwrite_destination': True
                             }

        print(f'Uploading {file} to {gcs_bucket}/{subfolder}')
        cloud_to_gcs(**cloud_to_gcs_args)


def sfmc_gcs_to_bq(destination_project_id, destination_bucket, dataset, gcp_key_path, **context):
    '''
    Args:
        destination_project_id:
        destination_bucket:
        dataset:
        gcp_key_path:
        **context:

    Returns:

    '''
    import sys, os
    from salesforce_marketing_cloud.gcs_to_bq import gcs_to_bq
    sys.path.insert(1, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    from dataengineering.check_files import check_files

    sfmc_feeds = ['sfmc_all_bu', 'sfmc_level2', 'sfmc_pilots']
    bq_load_params = {
        'field_delimiter': "|",
        'field_qualifier': '"',
        'skip_leading_rows': 1,
        'file_type_filter': "csv",
        'force_string': True,
        'project_id': destination_project_id,
        'destination_dataset': dataset,
        'source': destination_bucket,
        'blob_path': '',
        'write_disposition': "WRITE_TRUNCATE",
        'files': {}
        # 'add_table_suffix': ""
        # 'add_table_prefix': 'test_'
    }

    for feed in sfmc_feeds:
        print(f'Loading {feed} files')
        subfolder = feed
        bq_load_params['blob_path'] = subfolder + '/'
        # check_files() throws an error if it doesn't detect any files
        try:
            inbound_blobs = check_files('gcs', destination_bucket, subfolder=subfolder, gcp_key_path=gcp_key_path)
        except Exception:
            continue

        for file in inbound_blobs.keys():
            # bq_load_params['add_table_prefix'] = subfolder[5:]
            table_name = subfolder[5:] + '_' + file.split('.')[0].lower()

            print(f'Loading {file} to {table_name}...')
            bq_load_params['files'] = {f'{file}': f'{table_name}'}
            gcs_to_bq(gcp_key_path=gcp_key_path, **bq_load_params)


def sfmc_stg_to_final(project_id, dataset, destination_project, destination_dataset, gcp_key_path, **context):
    """

    Args:
        project_id:
        dataset:
        destination_project:
        destination_dataset:
        gcp_key_path:
        **context:

    Returns: None

    """
    from google.cloud import bigquery

    # GCP credentials
    bigquery_client = bigquery.Client.from_service_account_json(json_credentials_path=gcp_key_path, project=project_id)

    for table in bigquery_client.list_tables(dataset):
        # print(table.table_id)
        copy_bq_table(project_id,
                      dataset,
                      table.table_id,
                      destination_project,
                      destination_dataset,
                      table.table_id,
                      gcp_key_path
        )

    print('Tables have been copied')
    bigquery_client.close()

def sfmc_gcs_archive(destination_project_id, gcp_key_path, destination_bucket):
    '''
    Args:
        destination_project_id:
        gcp_key_path:
        destination_bucket:

    Returns:

    '''

    import sys, os
    sys.path.insert(1, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    from dataengineering.gcs_archive import gcs_archive
    from datetime import date

    sfmc_feeds = ['sfmc_all_bu', 'sfmc_level2', 'sfmc_pilots']
    archive_pattern = date.today().strftime('%Y%m%d')

    for feed in sfmc_feeds:
        # using the new gcs_archive module from de lib
        subfolder = feed + '/'
        gcs_archive(destination_project_id, gcp_key_path, destination_bucket, archive_pattern, subfolder=subfolder)