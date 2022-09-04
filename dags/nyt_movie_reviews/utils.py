from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

import logging

def api_to_s3(path, access_key, secret_key, bucket_name, ds_nodash) -> None:
    import requests
    import json
    from tempfile import NamedTemporaryFile as ntf

    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {secret_key}".format(secret_key=secret_key)
    }
    url = "https://api.nytimes.com/svc/movies/v2/{}/all.json?order=by-opening-date".format(path)
    response = requests.get(url + "&api-key={}".format(access_key), headers=headers)
    data = response.json()

    # print(response)

    with ntf(mode='w', suffix=f"{ds_nodash}") as temp_json_file:
        json.dump(data, temp_json_file, ensure_ascii=False, indent=4)
        temp_json_file.flush() # save file to disk

        logging.info(f"Saved {path} data in json temp file.")

        s3_hook = S3Hook(aws_conn_id="s3_minio_conn")
        s3_hook.load_file(
            filename=temp_json_file.name,
            key=f"{path}/nyt_{path}_{ds_nodash}.json",
            bucket_name=bucket_name,
            replace=True
        )

def s3_to_postgres() -> None:

    # Step1: Connect to s3 bucket to get files
    # Loops through files in each folder and store them in df


    # Step 2: Connect to postgres using PostgresHook
    # Read the data from df and load them using PostGresOperator
    hook = PostgresHook(postgres_conn_id="postgres_localhost")
