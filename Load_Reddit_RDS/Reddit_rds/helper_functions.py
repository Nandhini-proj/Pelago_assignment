import logging
import boto3
import pandas as pd
from io import BytesIO,StringIO
import io
import gzip
import psycopg2
from sys import stdout
import ast
import json
from pandas.io.json import json_normalize
from datetime import date, timedelta, time , datetime

# Define logging for the function
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
if logger.handlers:
    logger.handlers = []
stream_handler = logging.StreamHandler(stdout)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

#-----------------------------------------------------------------------------
# This function will return the latest file name in an S3 bucket folder.
# :param bucket: Name of the S3 bucket.
# :param prefix: Only fetch keys that start with this prefix (folder  name).
#-----------------------------------------------------------------------------
def get_latest_file_name(bucket_name,prefix):
    try:
        s3_client = boto3.client('s3')
        objs = s3_client.list_objects_v2(Bucket=bucket_name)['Contents']
        shortlisted_files = dict()            
        for obj in objs:
            key = obj['Key']
            timestamp = obj['LastModified']
            # if key starts with folder name retrieve that key
            if key.startswith(prefix):              
                # Adding a new key value pair
                shortlisted_files.update( {key : timestamp} )   
        latest_filename = max(shortlisted_files, key=shortlisted_files.get)
        return latest_filename
    except Exception as e:
        logger.error("No files in the bucket.Please check")
        logger.error(e)

#-----------------------------------------------------------------------------
# This function will list the content of s3 object.
# :param bucket: Name of the S3 bucket.
# :param key: folder 
#------------------------------------------------------------------------------
def get_s3_object(bucket_name,key):
    try:
        s3_client = boto3.resource('s3')
        obj = s3_client.Object(bucket_name,key)
        n = obj.get()['Body'].read()
        gzipfile = io.BytesIO(n)
        gzipfile = gzip.GzipFile(fileobj=gzipfile) 
        df = pd.read_csv(gzipfile)
        return df
    except Exception as e:
        logger.error("Not able to retrieve the s3 object.Please check")
        logger.error(e)
        
#-----------------------------------------------------------------------------
# This function creates an upsert query which replaces existing data based 
# on primary key conflicts
# :param table: postgres table name
# :param database_columns: postgres table column names
# :param primary_key: postgres table primary key
#------------------------------------------------------------------------------    
def create_update_query(table,database_columns,primary_key):
    try:
        columns = ', '.join([f'{col}' for col in database_columns])
        constraint = ', '.join([f'{col}' for col in primary_key])
        placeholder = ', '.join([f'%({col})s' for col in database_columns])
        database_columns_upd = [x for x in database_columns if x not in primary_key]
        updates = ', '.join([f'{col} = EXCLUDED.{col}' for col in database_columns_upd])
        query = f"""INSERT INTO {table} ({columns}) 
                    VALUES ({placeholder}) 
                    ON CONFLICT ({constraint}) 
                    DO UPDATE SET {updates};"""
        query.split()
        query = ' '.join(query.split())
        return query
    except Exception as e:
        logger.error("Problem with constructing the upsert query.Please check")
        logger.error(e)

#-----------------------------------------------------------------------------
# This function will load the data to the postgres table.
# :param df: dataframe
# :param table: postgres table to be loaded
# :param connection: connection to postgres db
# :primary_key: primary key for postgres table
#------------------------------------------------------------------------------ 
def load_updates(df, table, connection,primary_key):
    try:
        cursor = connection.cursor()
        df1 = df.where((pd.notnull(df)), None)
        insert_values = df1.to_dict(orient='records')
        database_columns = df.columns
        ts_cols = ['last_updated_ts']
        database_columns = [x for x in database_columns if x not in ts_cols]
        primary_key=ast.literal_eval(primary_key)
        row_count = 0
        for row in insert_values:
            cursor.execute(create_update_query(table,database_columns,primary_key), row)
            row_count = row_count + 1
        connection.commit()
        logger.debug(f'Inserted {row_count} rows.')
        cursor.close()
        del cursor
        connection.close()
    except Exception as e:
        logger.error("Data not loaded to DB.Please check")
        logger.error(e)

#-----------------------------------------------------------------------------
# This function will unpack the list.
#------------------------------------------------------------------------------ 
def unpackList(input_list):
    li = ",".join( repr(i) for i in input_list)
    if li == '':
        return 'NA'
    else:
        return li