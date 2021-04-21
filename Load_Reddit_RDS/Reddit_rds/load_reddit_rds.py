import logging
import boto3
import pandas as pd
from io import BytesIO
import io
import gzip
import psycopg2
from sys import stdout
import os
import numpy as np
import json
import ast
from pandas.io.json import json_normalize
from datetime import date, timedelta, time , datetime
import os
from helper_functions import get_latest_file_name,get_s3_object,create_update_query,load_updates,unpackList

# Define logging for the function
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
if logger.handlers:
    logger.handlers = []
stream_handler = logging.StreamHandler(stdout)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

def LoadRedditRds(event,context):

    #-------------------------------------------------------------------
    # Load Reddit data to Datawarehouse. 
    #-------------------------------------------------------------------

    # Get the lambda environment variables
    sm_resource = boto3.client('secretsmanager')
    keys = sm_resource.get_secret_value( SecretId='reddit/rds/cred') 
    dict_keys = json.loads(keys['SecretString'])
    host = dict_keys['host']
    port = dict_keys['port']
    port = int(port)
    user = dict_keys['user_lambda']
    database = os.environ['DATABASE']
    schema = os.environ['SCHEMA']
    table = os.environ['TABLE']
    bucket = os.environ['BUCKET']
    folder = os.environ['FOLDER']
    primarykey = os.environ['PRIMARY_KEY']
    bucket_token = os.environ['BUCKET_TOKEN']
    
    # Force the connections to the DB instance to use SSL. 
    sm_token = boto3.client('rds',region_name='ap-southeast-1')
    token = sm_token.generate_db_auth_token(host,port,user)
        
    files = ['RDS-PELAGO/rds-combined-ca-bundle.pem']
    local_file_name = '/tmp/' + 'rds-combined-ca-bundle.pem'
    s3 = boto3.resource('s3')
        
    for file in files:
        try:
            s3.Bucket(bucket_token).download_file(file, local_file_name)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                logger.info("The object does not exist.")
            else:
                raise
                
    connection = psycopg2.connect(host=host
                                ,user=user
                                ,password=token
                                ,database=database
                                ,sslmode='verify-full'
                                ,sslrootcert=local_file_name)

    #-------------------------------------------------------------------
    # 1. Get the latest file from the S3 bucket.
    # 2. Clean the data.
    # 3. Load reddit post data to the Database. 
    #-------------------------------------------------------------------
    prefix = folder + "/"
    table=schema+"."+table
    latest_form_file = get_latest_file_name(bucket_name=bucket,prefix=prefix)
    df_reddit_posts = get_s3_object(bucket_name=bucket,key=latest_form_file)

    # Clean the treatment_tags column . Unpack the list .
    df_reddit_posts['treatment_tags'] = df_reddit_posts.apply(lambda row:unpackList(row['treatment_tags']),axis=1)
    
    #Load the data to the table
    load_updates(df_reddit_posts,table,connection,primarykey)
