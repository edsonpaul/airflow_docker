"""
Functions used in studybase pipeline
"""

def extract_postgre_tables(svc_password, ssl_root_cert, gcp_key_path, gcs_bucket, tablelist):

    import csv
    import datetime
    import time
    import psycopg2
    import logging
    from google.cloud import storage
    import pandas as pd
    import numpy as np

    logging.basicConfig(level=logging.INFO)
    schema = 'studybase'

    conn = psycopg2.connect(
        dbname='t2d',
        user='svc_etl_gmp',
        password=svc_password,
        host='prod-t2d-rds.c1nqtd6s9v6b.us-east-1.rds.amazonaws.com',
        port='5432',
        sslmode='verify-full',
        sslrootcert=ssl_root_cert
    )

    cursor = conn.cursor()

    # get all tables, columns
    cursor.execute("""
    SELECT
    table_name,
    column_name,
    ordinal_position,
    is_nullable,
    data_type
    FROM information_schema.columns
    WHERE table_schema = '{}'
    AND table_name <> 'errorlog'""".format(schema))
    records = cursor.fetchall()

    # create tables dict
    # each table is a list of columns with the keys below
    # name, type, position, nullable
    tables_columns = {}

    limit_more = True
    limit_list = tablelist
    
    for row in records:
        table_name = row[0]
        if (limit_more and table_name not in limit_list):
            continue
        col_name = row[1]
        col_pos = row[2]
        is_nullable = row[3]
        col_type = row[4]

        if table_name not in tables_columns.keys():
            tables_columns[table_name] = []
        tables_columns[table_name].append({
            'name': col_name,
            'type': col_type,
            'position': col_pos,
            'nullable': is_nullable
        })

    for table, columns in tables_columns.items():
        sorted_order = sorted(columns, key=lambda kv: kv['position'])
        tables_columns[table] = sorted_order

    client = storage.Client.from_service_account_json(gcp_key_path)
    bucket = client.get_bucket(gcs_bucket)
    ts = time.time()
    file_ts = datetime.datetime.fromtimestamp(ts).strftime('%Y_%m_%d_%H_%M_%S')

    for table, column_dict in tables_columns.items():
        logging.info('Starting table {}'.format(table))
        sql = "SELECT * FROM {}.{}".format(schema, table)
        result = pd.read_sql(sql, conn)
        df = pd.DataFrame(result)
    
        filepath = '{}_{}.csv'.format(table, file_ts)
        bucket.blob(filepath).upload_from_string(df.to_csv(sep=",", line_terminator='\r\n', float_format='%g', index=False, encoding="utf-8"), 'text/csv')

        # drop indexes from dataframe
        df = df.reset_index(drop=True)

    #close connection
    conn.close()
