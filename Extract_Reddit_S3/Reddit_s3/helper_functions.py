# helper_functions.py
import pandas as pd
from datetime import time,datetime,date
from io import BytesIO,TextIOWrapper
from sys import stdout
import json
import logging
import sys
import boto3
import gzip

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
if logger.handlers:
    logger.handlers = []
stream_handler = logging.StreamHandler(stdout)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

#---------------------------------------------------------------------------------
# Copy the dataframe to S3 files.
#---------------------------------------------------------------------------------
def Dataframe_to_s3(s3_resource, input_datafame, bucket_name, fileName, format):
    if format == 'csv':
        gz_buffer = BytesIO()
        with gzip.GzipFile(mode='w', fileobj=gz_buffer) as gz_file:
            input_datafame.to_csv(TextIOWrapper(gz_file, 'utf8'), index=False)
    
    s3_resource.Object(bucket_name, fileName).put(Body=gz_buffer.getvalue())
    logger.info("Copied the dataframe to AWS S3 bucket")

#---------------------------------------------------------------------------
# Get datetime.
#---------------------------------------------------------------------------
def getDatetime(datekey):
    date_time = datetime.fromtimestamp(datekey)
    return date_time

#---------------------------------------------------------------------------
# Get date.
#---------------------------------------------------------------------------
def getDate():
    dt = datetime.today().strftime('%Y-%m-%d')
    return dt

#---------------------------------------------------------------------------
# Get date hour.
#---------------------------------------------------------------------------
def getDateHour():
    hour = datetime.today().strftime('%H')
    return hour