__author__ = "Larrie"
__credits__ = [""]

"""gcs_to_bq module - current version is beta
	This module handles most cases of text files ETL from GCS to the GCP_etl
	dataset.  Most of these types of files will be delimited in nature. The
	module assumes the calling program has created a GCP credentials object
	and has noted its name in the kwargs. The module performs these functions
	basis the supplied kwargs:
	1. Develops a manifest of all files to be transferred; or, uses a
		supplied list | dict of specific tables to be transferred.
	2. If the file has row 1 headings, the module will discover and normalize
		a dictionary for the file to be loaded.
	3. If the target table is already created, the existing information schema
		is compared to the new file dictionary (column names only) presuming the
		target table names are the same. If the dictionaries do not match, the
		existing table is dropped and re-created with the new dictionary.
	4. The manifest can be saved to a GCS bucket location as a json file.
	5. See test_gcs_to_bq.py for example of how to implement

	Modules used:
		Some functions utilize pandas.

	Usage: from gcs_to_bq import *
	 		develop the necessary kwargs
	 		<name> = gcs_to_bq(**kwargs)
	returns: manifest list of files imported
	kwargs: defined below

	TODO:
		1. Add logging to OC supplied module.
		2. Implement the ability to save the created dictionary objects to GCS bucket.
		3. Add alerts for when a schema change is detected or a table (new file is to be added).
		4. Add support for prn files, but this might just be another form of ragged right implementation.
		5. Support for other file type forms, such as json.
		6.*Capture and save the sniffed delimiters if they differ from kwargs values. (handled, ready for testing)
"""

from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound
from datetime import datetime
import io
import csv
import re
import json


class Def_Dialect(csv.Dialect):
    delimiter = ','
    doublequote = True
    lineterminator = '\r\n'
    quotechar = '"'
    quoting = csv.QUOTE_MINIMAL


def remove_text_after_delimiter(obj, delim):
    """ Handles strip off trailing dates from filenames
        Used when ingesting files

    Arguments:
        obj filename portion (not extension)
        delim delimiter used to separate

    Returns:
        obj name with trailing date stripped off
    """
    ltxt = len(obj)
    rtxt = obj[::-1]
    rloc = rtxt.find(delim)
    ntxt = rtxt[rloc + 1:ltxt]
    rtxt = ntxt[::-1]
    return rtxt


def camel_to_snake(name):
    """
    Convert a name to snake format

    Args:
        name string value to convert

    Return:
        converted string value
    """

    name0 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    name1 = name0.replace(' ', '')
    name2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name1).lower()
    return name2.replace("__", "_")


def get_GCP_Client(**kwargs):
    """
    Get a GCP client of type_client (GCS | GCP) basis the
    path to the credentials json from OC

    KWArgs:
        credentials - points to GCP creds object
        type_client= type of GCP client (GCS | GCP)

    Returns:
        GCP client object
    """
    my_creds = kwargs["credentials"]
    ggg_client = None
    if kwargs["type_client"] in ['GCP', 'GCS']:
        if kwargs["type_client"] == 'GCP':
            ggg_client = bigquery.Client(credentials=my_creds, project=my_creds.project_id, )
        if kwargs["type_client"] == 'GCS':
            ggg_client = storage.Client(credentials=my_creds)
    return ggg_client


def prepare_Job_Config(dialect, **kwargs):
    """
    The prepare_Job_Config returns a Google load job configuration

    Arguments:
        dialect is dictionary of discovered file delimiters

    KWArgs:
        skip_leading_rows - number of header rows to skip
        file_ingest_delimiter - when '`' (back tick) then doing RR full rows
        field_delimiter - field delimiter, default is comma ','
        field_qualifier - quoted string delimiter, default is double quote

    Returns:
        obj name with GCP Job Config
    """

    job_config = bigquery.LoadJobConfig()  # Instantiate a job configuration
    job_config.ignore_unknown_values = True  # Ignore stuff - error avoidence
    job_config.max_bad_records = 100  # Permit up to 100 error rows
    if kwargs["write_disposition"] == "WRITE_APPEND":
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    else:
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    # only delimited gets the following
    if kwargs["mode"] in ['csv', 'ragged_right']:  # Handle delimited
        if kwargs["skip_leading_rows"] > 0:  # When diff than default
            job_config.skip_leading_rows = kwargs["skip_leading_rows"]  # User supplied value
        if kwargs["file_ingest_delimiter"] == "`":  # Special case, when backtick, full row
            job_config.allowQuotedNewlines = False  # so, cannot specify quoted newlines
            job_config.field_delimiter = kwargs["file_ingest_delimiter"]  # Specify user supplied ingestion delimiter
        else:  # otherwise,
            job_config.allowQuotedNewlines = True  # Allow embedded CRLF - but scrub out later
            if len(dialect) > 0:  # We have a sniffed set of delimiters
                if "dialect_field_delimiter" in dialect:
                    job_config.field_delimiter = dialect["dialect_field_delimiter"]  # Specify discovered delimiter
                if "dialect_quote_delimiter" in dialect:
                    job_config.quote_character = dialect["dialect_quote_delimiter"]  # ditto
            else:
                job_config.field_delimiter = kwargs["field_delimiter"]  # Specify user supplied delimiter
                if len(kwargs["field_qualifier"]) == 1:
                    job_config.quote_character = kwargs["field_qualifier"]  # if non-standard quote char specified
        job_config.source_format = bigquery.SourceFormat.CSV  # Presumed delimited file
    return job_config


def list_to_json(list_in):
    """
    Convert internal formatted list of dict() to json

    Args:
        item list of dicts

    Return:
        converted json string
    """

    dd_obj = list_in[0]  # start with a dd of inbound from the root obj
    my_dd = list(dd_obj.keys())  # local dd
    json_str = "[\n"  # initialize the result
    one_time = True  # switch to control first item no comma

    for dd in list_in:
        if one_time:  # no comma preceding the first itme
            tmp_obj = ' {'
            one_time = False  # then commas all the way down
        else:
            tmp_obj = ',{'

        comma = ","  # insert comma all items but last
        for ix in range(len(my_dd)):
            val = my_dd[ix]
            if val == my_dd[-1]:
                comma = ""  # last item dished up
            tmp_obj = tmp_obj + '"{}":"{}"{}'.format(val, dd[val], comma)
        tmp_obj = tmp_obj + '}\n'
        json_str = json_str + tmp_obj
    json_str = json_str + ']'
    return json_str


def build_json_schema(data_dict):
    """
    Convert internal formatted dictionary to json

    Args:
        dictionary item list of column dict to convert

    Return:
        converted json string
    """

    json_dict = "[\n"

    for dd in data_dict:
        if dd["tgt_col_ord"] == 1:
            tmp_obj = ' {'
        else:
            tmp_obj = ',{'
        tmp_obj = tmp_obj + '"description":"' + dd["src_file_name"] + '~' + dd["src_col_name"] + \
                  '","name":"' + dd["tgt_col_name"] + \
                  '","type":"' + dd["tgt_col_type"] + \
                  '","mode":"' + dd["tgt_col_mode"] + '"}\n'
        json_dict = json_dict + tmp_obj
    json_dict = json_dict + ']\n'
    return json_dict


def make_manifest(gcs_client=None, **kwargs):
    """
    Make manifest returns a list of all files in the targeted source blob'
    GCS blobs are addressed by a blob path and a filename as a complete
    string. <bucket>/<optional: 1 or more 'subfolders' delimited by />/<filename>.<filetype>

    Default Args:
        gcs_client = GCS client object from OC

    KWArgs:
        source = name of GCP bucket
        blob_path = "" optional blob path to files
        files = [] optional list of files or {} of file:table to do
        file_prefix = "" optional string prefix file name
        file_type_filter = "" optional string of type of file, if specified only specified file type considered
        add_table_prefix = "" optional prefix appended before table name
        add_table_suffix = "" optional suffix appended after table name
        remove_file_prefix = "" optional "yes" to invoke; removes file_prefix
        remove_file_trailing_delimiter = "" optional delimiter preceding text

    Returns:
        manifest list of dictionary blobs found matching the criteria
    """

    my_manifest = []  # placeholder for outcome
    src_dict = {}  # placeholder for when src files is used
    run_date = datetime.now()  # datetime of execution (consumed later)
    files = kwargs["files"]

    if len(files) > 0:  # do we have source files specified?
        if type(files) is list:  # yes, is it a list of files?
            for obj in files:
                src_dict.update({obj: ''})
        elif type(files) is dict:  # yes, is it a dict(file:table) of files?
            src_dict = files
        else:
            return my_manifest  # yes, but neither list nor dict so ignore

    blobs = gcs_client.list_blobs(kwargs["source"])  # Excluding folder inside bucket
    for blob in blobs:
        if blob.name[-1] == '/':
            continue  # ignore roots of folder blobs
        if 'blob_path' in kwargs and \
                len(kwargs["blob_path"]) > 0 and \
                kwargs["blob_path"] not in blob.name:
            continue  # optional - ignore blobs whose path we're not interested in
        else:  # ok, we have a candidate file name
            if 'blob_path' in kwargs and \
                    len(kwargs["blob_path"]) > 0:
                obj = blob.name.replace(kwargs["blob_path"], "")
            else:
                obj = blob.name
            # obj now contains a candidate file name sans blob

            table_name = ""  # presume no dict specifying name in play

            # Prepare the table_name
            if len(src_dict) > 0:  # if using a specified list, test against it
                if obj in src_dict.keys():
                    table_name = src_dict[obj]
                else:
                    continue  # move along folks, nothing of interest here

            # continue on, obj is our target, we may or may not have a table name

            obj_parts = obj.split('.')  # Break out the file name parts
            if len(table_name) == 0:  # No table name specified, develop one from obj
                table_name = obj_parts[0]  # Start with the basics
                if 'file_prefix' in kwargs and \
                        len(kwargs["file_prefix"]) > 0 and \
                        kwargs["file_prefix"] != obj[0:len(kwargs["file_prefix"])]:
                    continue  # optional - ignore files not having specified prefix
                if 'file_type_filter' in kwargs and \
                        len(kwargs["file_type_filter"]) > 0:
                    if obj_parts[-1] != kwargs["file_type_filter"]:
                        continue  # optional - ignore files not having specified file type

                if 'remove_file_prefix' in kwargs and \
                        kwargs["remove_file_prefix"] == "yes":
                    table_name = table_name.replace(kwargs["file_prefix"], "")
                if 'remove_file_trailing_delimiter' in kwargs and \
                        len(kwargs["remove_file_trailing_delimiter"]) == 1:
                    table_name = remove_text_after_delimiter(table_name,
                                                                 kwargs["remove_file_trailing_delimiter"])
                table_name = "{}{}{}".format(kwargs["add_table_prefix"],
                                                 table_name,
                                                 kwargs["add_table_suffix"])

            blob_dict = {'blob_name': blob.name,
                         'table_name': table_name.lower(),
                         'file_name': obj,
                         'is_table_new': 'n',
                         'has_table_schema_changed': 'n',
                         'table_loaded_successfully': 'y',
                         'table_rows_loaded': 0,
                         'dialect_field_delimiter': None,
                         'dialect_quote_delimiter': None,
                         'table_load_date': run_date.strftime("%Y%m%d_%H%M%S")}
            my_manifest.append(blob_dict)
    return my_manifest


def save_manifest(manifest=None, gcs_client=None, tgt_file_name=None, **kwargs):
    """
    Save the created manifest to GCS bucket

    Args:
        manifest - obj list created by Make_Manifest
        gcs_client - GCS client object from OC

    KWArgs:

    Return:
        GCS Path to manifest
    """

    if tgt_file_name is None:
        tgt_bucket_uri = None
    else:
        manifest_json = list_to_json(manifest)
        tgt_bucket_uri = "gs://{}/{}{}".format(kwargs["source"], kwargs["save_manifest"], tgt_file_name)
        bucket = gcs_client.bucket(kwargs["source"])
        blob = bucket.blob(kwargs["save_manifest"] + tgt_file_name)
        blob.upload_from_string(manifest_json)

    return tgt_bucket_uri


def make_dictionary(manifest=None, dictionary=None, gcs_client=None, **kwargs):
    """ Create an internal data dictionary object for the supplied blob.
    Process has two options:
        1. Discover and create dictionary list. Presumed to be a delimited file; if the specified
            column delimiter is not in the list of standard sniffed delimiters, then it is appended.
        2. Prepare dictionary list from supplied dictionary file.
            Requires a formatted csv containing;
             table_name, file_name, column_name, data_type for BQ, nullable, ordinal_position
            Also requires: field_delimiter and field_qualifier

    Default Args:
        manifest = dict(blob_name, table_name, file_name)
        gcs_client = GCS client object from OC

    KWArgs:
        source - name of GCP bucket where data is
        use_datadict - blob name of dictionary data in bucket

    TODO:
        1. Might want to log the fact that the sniffed dialect is diff kwargs

    Returns:
        1. dialect alteration to kwargs as a dictionary
        2. list of all columns in the targeted source file blob (the dictionary)
    """

    my_dictionary = []  # Null state to kick it off
    dict_dialect = {}  # ditto
    encoding = 'utf-8-sig'
    default_data_type = 'STRING'
    default_data_mode = 'NULLABLE'
    default_delimiters = [',', ';', ':', '|', '\t']  # Used by csv.sniffer
    default_quoters = ['"', "'", '\t']  # Used internally
    dialect = None
    FIELD_MATCHER = re.compile(r'(^[^a-zA-Z])|(\W)')

    if dictionary is None:  # Handle case when schema needs to be discovered
        if kwargs["field_qualifier"] not in default_quoters:
            dialect = Def_Dialect  # start of oddball dialect
            dialect.delimiter = kwargs["field_delimiter"]
            dialect.quotechar = kwargs["field_qualifier"]
            csv.register_dialect('rfc', dialect)
        if kwargs["field_delimiter"] not in default_delimiters:
            default_delimiters.append(kwargs["field_delimiter"])
        bucket = gcs_client.get_bucket(kwargs["source"])  # address the bucket
        src_blob = bucket.get_blob(manifest["blob_name"])  # and the blog of interest
        content = src_blob.download_as_string(start=0, end=10240)  # Read a chunk of bytes to include all header data
        bytes_buffer = io.BytesIO(content)  # Turn into bytes
        wrapped_text = io.TextIOWrapper(bytes_buffer, encoding=encoding, newline='\n')

        wasSniffed = False
        if dialect is None:
            try:
                dialect = csv.Sniffer().sniff(wrapped_text.read(), delimiters=default_delimiters)
                wasSniffed = True
            except csv.Error:
                dialect = Def_Dialect  # start of oddball dialect
                csv.register_dialect('rfc', dialect)
        if wasSniffed:  # Only save if actual sniff
            if dialect.delimiter != kwargs["field_delimiter"]:  # Only save delimiter changes
                upd = {'dialect_field_delimiter': dialect.delimiter}
                dict_dialect.update(upd)
            if dialect.quotechar != kwargs["field_qualifier"]:
                upd = {'dialect_quote_delimiter': dialect.quotechar}
                dict_dialect.update(upd)
        wrapped_text.seek(0)  # Point to the origin
        reader = csv.reader(wrapped_text, dialect)  # Read 1st line
        hdr_line = next(reader)
        ord_pos = 1  # Init Ordinal Position
        #print(hdr_line)
        for col in hdr_line:
            c0_name = col.strip().replace("/", "_").replace(" ", "_").replace("-", "_")
            mini_dict = {'table_name': manifest["table_name"],
                         'src_file_name': manifest["file_name"],
                         'src_col_name': col,
                         'tgt_col_name': FIELD_MATCHER.sub("_",camel_to_snake(c0_name)),
                         'tgt_col_type': default_data_type,
                         'tgt_col_mode': default_data_mode,
                         'tgt_col_ord': ord_pos}
            ord_pos += 1
            my_dictionary.append(mini_dict)
    else:  # Handle case when an external dictionary is in play
        for i, row in dictionary.iterrows():
            if row["table_name"] != manifest["table_name"] and row["file_name"] != manifest["file_name"]:
                continue
            col = row["column_name"]
            c0_name = col.strip().replace("/", "_").replace(" ", "_").replace("-", "_")
            mini_dict = {'table_name': manifest["table_name"],
                         'src_file_name': manifest["file_name"],
                         'src_col_name': col,
                         'tgt_col_name': FIELD_MATCHER.sub("_",camel_to_snake(c0_name)),
                         'tgt_col_type': row["data_type"],
                         'tgt_col_mode': row["nullable"],
                         'tgt_col_ord': row["ordinal_position"]}
            my_dictionary.append(mini_dict)

    return dict_dialect, my_dictionary


def get_existing_information_schema(gcp_client=None, table_name=None, **kwargs):
    """
    Retrieve an existing table information schema and return a dataframe.

    Default Args:
        gcp_client - GCP client from OC
        table_name str of targeted table from OC

    KWArgs:
        project_id, destination_dataset

    Return:
        dataframe containing information_schema data
    """

    info_schema_basis = """
	select ordinal_position, table_name, column_name
      from `<gcp_project_id>.<gcp_dataset>.INFORMATION_SCHEMA.COLUMNS`
     where table_name = '<gcp_table>'
       and column_name not like 'uhgrd_%'
     order by table_name,ordinal_position;	
	"""

    schema_sql = info_schema_basis.replace("<gcp_project_id>", kwargs["project_id"])
    schema_sql = schema_sql.replace("<gcp_dataset>", kwargs["destination_dataset"])
    schema_sql = schema_sql.replace("<gcp_table>", table_name)
    schema_df = gcp_client.query(schema_sql).to_dataframe()

    return schema_df


def diff_schemas(dictionary, ex_scma_df):
    """
    diff the new and existing schemas and return True (same) False (different).

    Uses:
        pandas

    Default Args:
        dictionary is internal format of list of dict items from OC
        ex_scma_df is a datafram of the existing table information schema

    Return:
        True (same) False (different)
    """
    import pandas as pd

    result = True  # presume they are the same

    # First order of business is to create a matching DF of the dictionary
    # Cannot compare the data types as some existing data types have been set
    tbl_df = pd.DataFrame([])
    op = 1
    for dd in dictionary:
        dd_item = {'ordinal_position': op,
                   'table_name': dd["table_name"],
                   'column_name': dd["tgt_col_name"]}
        tbl_df = tbl_df.append(pd.DataFrame(dd_item, index=[0]), ignore_index=True)
        op += 1

    # Now that we have comparable df, we can diff the dataframes
    df_diff = pd.concat([tbl_df, ex_scma_df]).drop_duplicates(keep=False)
    if len(df_diff) > 0:
        result = False
    return result


def get_external_dictionary(gcs_client=None, **kwargs):
    """
    Retrieve a external dictionary from GCS (csv) and return a dataframe.

    Uses:
        pandas for csv read

    Format:
        CSV, with R1 column headings, values to match manifest
        table_name, file_name, column_name, data_type for BQ, nullable, ordinal_position

    Default Args:
        gcs_client - GCS client from OC

    KWArgs:
        bucket - name of bucket where data is
        use_datadict - blob_path and /filename of supplied data dictionary in csv format

    Return:
        dataframe containing dictionary data
    """
    import pandas as pd

    source_bucket_uri = "gs://{}/{}".format(kwargs["source"], kwargs["use_datadict"])
    ext_dictionary = pd.read_csv(source_bucket_uri)

    return ext_dictionary


def load_files(manifest=None, dictionary=None, gcp_client=None, gcs_client=None, **kwargs):
    """
    Load files loads the files specified in the manifest into the
    project_etl dataset.

    Default Args:
        manifest = dict(blob_name, table_name, file_name)
        dictionary = dataframe of external dictionary
        gcp_client = GCP client object from OC
        gcs_client = GCS client object from OC

    KWArgs:
        mode - type of source file; default is csv
        skip_leading_rows - number of rows to skip; def = 0 (no header row)

    Returns:
        updated manifest item to update changes
    """

    def create_table(client, tgt_table_id, dictionary):
        """
        Internal function to manage creation of tables
        """
        tbl_schema = json.loads(build_json_schema(dictionary))
        new_table = bigquery.Table(tgt_table_id, schema=tbl_schema)
        result = client.create_table(new_table)
        return

    def delete_table(client, tgt_table_id):
        """
        Internal function to manage deletion of tables
        """
        del_table = bigquery.Table(tgt_table_id)
        result = client.delete_table(del_table)
        return

    #print(manifest)
    load_results = manifest  # Prime the return manifest dict
    dialect = {}             # And a dialect in case we don't get one
    tgt_table_id = "{}.{}.{}".format(kwargs["project_id"], kwargs["destination_dataset"],
                                     manifest["table_name"])

    if dictionary is None:   # No inbound external dictionary
        tbl_dictionary = None
    else:  # Whoomp, there it is! Hit me! (better do something with it: I'm takin' it back to the old school)
        dialect, tbl_dictionary = make_dictionary(manifest=manifest, dictionary=dictionary,
                                                  gcs_client=gcs_client, **kwargs)

    if kwargs["skip_leading_rows"] > 0:  # implies first row names in play
        if tbl_dictionary is None:       # implies we didn't have an external dictionary
            dialect, tbl_dictionary = make_dictionary(manifest=manifest, gcs_client=gcs_client, **kwargs)
            #print(tbl_dictionary)
        if len(tbl_dictionary) > 0:
            schemaExist = True
            try:
                tbl_result = gcp_client.get_table(tgt_table_id)
                exist_schema = get_existing_information_schema(gcp_client=gcp_client,
                                                               table_name=manifest["table_name"],
                                                               **kwargs)
                result = diff_schemas(tbl_dictionary, exist_schema)
                if not result:  # diff failed
                    # todo: alert?
                    delete_table(gcp_client, tgt_table_id)  # drop existing table
                    create_table(gcp_client, tgt_table_id, tbl_dictionary)  # and create with new schema
                    upd_item = {'has_table_schema_changed': 'y'}  # inform OC of status
                    load_results.update(upd_item)
            except NotFound:
                # todo: new file alert?
                create_table(gcp_client, tgt_table_id, tbl_dictionary)
                upd_item = {'is_table_new': 'y'}  # inform OC of status
                load_results.update(upd_item)
        else:
            schemaExist = False

        job_config = prepare_Job_Config(dialect, **kwargs)
        if schemaExist:
            pass
        else:
            job_config.autodetect = True  # Processing w/o schemas; turn on autodetect

    else:
        # todo: develop dictionary when not have row 1 headings
        # implies having some sort of base(displacement) object (PRN files)
        # or, for delim files, an external dictionary
        return None

    source_bucket_uri = "gs://{}/{}".format(kwargs["source"], manifest["blob_name"])
    load_job = gcp_client.load_table_from_uri(source_bucket_uri, tgt_table_id, job_config=job_config)
    load_job_results = load_job.result()
    destination_table = gcp_client.get_table(tgt_table_id)
    upd_item = {'table_rows_loaded': destination_table.num_rows}  # inform OC of rows loaded
    load_results.update(upd_item)  # inform OC of status
    if len(dialect) > 0:  # Inform OC of dialect
        load_results.update(dialect)  # for file being loaded
    return load_results


def gcs_to_bq(gcp_key_path, **kwargs):
    """
    Serves as the main callable function for the module.
    Default KWArgs are merged to complete the set.

    Arguments:
        gcp_cred_path - Req'd -path to gcp credentials from OC


    Default Arguments:
        mode="csv"|"ragged_right" | "prn" (implies some sort of base:displacement dictionary)
        write_disposition: "WRITE_TRUNCATE" | "WRITE_APPEND". Defaults to "WRITE_TRUNCATE"
        files=[list of files] | null list [] for all files | {} where key is file, value is table
        field_delimiter=","
        field_qualifier='"'
        file_ingest_delimiter=""  defines the delimiter when doing ragged right
        skip_leading_rows=0 | integer
        file_prefix=None | "string"
        file_type_filter="" | "string"
        remove_file_prefix="" | "yes
        remove_file_trailing_delimiter='delimiter' - delimited by _date
        add_table_prefix="" | "string"
        add_table_suffix="" | "string"
        force_string=false | true
        save_manifest="no" | blob_path in bucket of optumlabs_manifest.json
        save_datadict="no" | blob_path in bucket of optumlabs_datadict_<table_name>.json
        use_datadict="no" | blob_path and /filename of supplied data dictionary in csv format


    KWArgs:
        project_id=gcs/gcp project string
        destination_dataset=name of the targeted dataset (ie. <something>_etl)
        source= GCP bucket name
        blob_path=blob full path (folder) containing the files
        files=[list of file] | {file:table} | wildcard string
    Return:
         list of files imported
    """

    default_kwargs = {'mode': "csv",
                      'write_disposition': "WRITE_TRUNCATE",
                      'files': [],
                      'field_delimiter': ",",
                      'field_qualifier': '"',
                      'file_ingest_delimiter': '',
                      'skip_leading_rows': 0,
                      'file_prefix': "",
                      'file_type_filter': "",
                      'add_table_prefix': "",
                      'add_table_suffix': "",
                      'remove_file_prefix': "",
                      'remove_file_trailing_delimiter': "",
                      'force_string': False,
                      'save_manifest': 'no',
                      'save_datadict': 'no',
                      'use_datadict': 'no'
                      }
    my_kwargs = {**default_kwargs, **kwargs}  # Assure using the full set of kwargs

    """
    2021 Jan 22 - change to obtain self GCP credentials object, we now get cred path inbound rather than cred object
    """

    import os
    from pathlib import Path
    from google.oauth2 import service_account
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(Path(gcp_key_path))
    credentials = service_account.Credentials.from_service_account_file(str(Path(gcp_key_path)), scopes=[
        "https://www.googleapis.com/auth/cloud-platform"], )

    gcs_client = get_GCP_Client(credentials=credentials, type_client='GCS')
    my_manifest = make_manifest(gcs_client=gcs_client, **my_kwargs)

    if len(my_manifest) > 0:  # Anything in the bucket? If so, process 1x1
        gcp_client = get_GCP_Client(credentials=credentials, type_client='GCP')
        if my_kwargs["mode"] == "ragged_right":  # Ragged right has special processing
            delta_kwargs = {'file_ingest_delimiter': '`'}  # turn on back tick for full string
            my_kwargs.update(delta_kwargs)  # inform

        ext_dictionary = None  # Placeholder for null case
        if my_kwargs["use_datadict"] != "no":
            ext_dictionary = get_external_dictionary(gcs_client=gcs_client, **my_kwargs)

        ix = 0  # track manifest item to enable update
        for src_obj in my_manifest:  # src_obj is dict(blob_name, table_name, file_name)
            if my_kwargs["mode"] in ["csv", "ragged_right"]:
                load_result = load_files(manifest=src_obj,
                                         dictionary=ext_dictionary,
                                         gcp_client=gcp_client,
                                         gcs_client=gcs_client,
                                         **my_kwargs)
                my_manifest[ix] = load_result  # capture outcome to manifest
                #print(load_result)
            ix += 1

        if my_kwargs["save_manifest"] != "no" and len(my_manifest) > 0:
            manifest_path = save_manifest(manifest=my_manifest,
                                          gcs_client=gcs_client,
                                          tgt_file_name='optumlabs_manifest.json',  # optumlabS bc kevin says so
                                          **kwargs)
    # print(manifest_path)

    return my_manifest
