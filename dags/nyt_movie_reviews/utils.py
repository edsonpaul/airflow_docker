from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

import json
from tempfile import NamedTemporaryFile
import logging

def api_to_s3(path, access_key, secret_key, bucket_name, ds_nodash) -> None:
    import requests

    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {secret_key}".format(secret_key=secret_key)
    }
    url = "https://api.nytimes.com/svc/movies/v2/{}/all.json?order=by-opening-date".format(path)
    response = requests.get(url + "&api-key={}".format(access_key), headers=headers)
    data = response.json()

    # print(response)

    with NamedTemporaryFile(mode='w', suffix=f"{ds_nodash}") as temp_json_file:
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


def s3_to_postgres(path, bucket_name, ds_nodash) -> None:
    import pandas as pd
    import os
    import psycopg2
    import psycopg2.extras as extras
    import numpy as np

    # Step1: Connect to s3 bucket to get files
    # Loops through files in each folder and store them in df
    s3_hook = S3Hook(aws_conn_id="s3_minio_conn")
    #postgres_hook = PostgresHook(postgres_conn_id="postgres_localhost")

    key=f"{path}/nyt_{path}_{ds_nodash}.json"
    try:
        file_name = s3_hook.download_file(key, bucket_name)
    except Exception as error:
        print(error)

    print(f"Successfully downloaded {file_name}")

    # Rename downloaded file from the arbitrary file name created by s3_hook download
    downloaded_file_path = '/'.join(file_name.split('/')[:-1])
    new_file_name = f"{downloaded_file_path}/{path}.json"
    os.rename(src=file_name, dst=new_file_name)

    print(f"After rename: {new_file_name}.")
    
    with open(new_file_name, 'r') as j:
        contents = json.loads(j.read())
    
    
    df = pd.DataFrame()
    for result in contents['results']:
        df = df.append(result, ignore_index=True)
    # Use dataframe apply for json field
    df['multimedia'] = df['multimedia'].apply(json.dumps)

    # df = df.replace(r'\r+|\n+|\t+','', regex=True)
    # df = df.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["",""], regex=True, inplace=True)

    try:
        conn = psycopg2.connect(
            host="host.docker.internal",
            database="test",
            user="airflow",
            password="airflow")
        
        # create a cursor
        cursor = conn.cursor()
        
	    # execute a statement
        print('PostgreSQL database version:')
        #cursor.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cursor.fetchone()
        print("DB version {}".format(db_version))
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    
    logging.info("Connection to postgres established.")

    # create the table if it does not already exist
    # cursor.execute(open(sql_query, "r").read())
    # conn.commit()

    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    # SQL query to execute insert
    query = "INSERT INTO %s(%s) VALUES %%s" % (path, cols) # path is also the tables name

    extras.register_default_json(loads=lambda x: x)
    extras.execute_values(cursor, query, tuples)
    conn.commit()

    cursor.close()

    if conn is not None:
        conn.close()
        print('Database connection closed.')



    # DATABASE_LOCATION = 'postgresql://airflow:airflow@host.docker.internal:5432/test'
    # DATABASE_LOCATION = 'postgresql://airflow:airflow@localhost:5432/test'
    # engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    # print(f"Engine: {engine}")
    # df.to_sql(
    #     "critics",
    #     engine,
    #     schema='public',
    #     index=False,
    #     if_exists='append',
    #     dtype={"bets": sqlalchemy.types.JSON}
    # )

    # try:
    #     logging.info("Trying to load the data.")
        
    # except:
    #     print("Data already exists in the database")