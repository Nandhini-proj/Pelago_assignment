import pandas as pd
from datetime import time,datetime
import time
import logging 
from sys import stdout
import sys
import os 
import boto3
import gzip 
import praw
import json
from reddit_class import API 
from helper_functions import Dataframe_to_s3,getDatetime,getDate,getDateHour

# Define logging for the function
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
if logger.handlers:
    logger.handlers = []
stream_handler = logging.StreamHandler(stdout)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

def ExtractTopRedditPostsToS3(event,context):
   
    # Get the lamba environment variables
    sm_resource = boto3.client('secretsmanager')
    keys = sm_resource.get_secret_value( SecretId='pelago/reddit/cred')
    dict_keys = json.loads(keys['SecretString'])
    client_id = dict_keys['client_id']
    client_secret = dict_keys['client_secret']
    bucketName = os.environ['REDDIT_BUCKET']
    topic = os.environ['REDDIT_TOPIC']
    file_format = os.environ['FILE_FORMAT']
    timestr = time.strftime("%Y%m%d%H%M%S")
    s3_resource  = boto3.resource('s3')
    
    cred_dict = {'client_id' :client_id
                ,'client_secret':client_secret
                ,'topic':topic}
    
    # Call the API class to get the api details 
    api = API(**cred_dict)

    #---------------------------------------------------
    # Extract the top 100 Reddit posts.
    #---------------------------------------------------
    api.subreddit.createDf()
    df_RedditTopPost = api.subreddit.format_reddit_posts
    df_RedditTopPost['created_datetime'] = df_RedditTopPost.apply(lambda row:getDatetime(row['created']),axis=1)
    df_RedditTopPost['current_dt'] = df_RedditTopPost.apply(lambda row:getDate(),axis=1)
    df_RedditTopPost['current_hr'] = df_RedditTopPost.apply(lambda row:getDateHour(),axis=1)
    df_RedditTopPost.drop('created',axis=1,inplace=True)
    
    # Log Error if Reddit top post are not retrieved
    if df_RedditTopPost is not None:
        logger.info(" Shape of the reddit top post retrieved  :%d X %d", *df_RedditTopPost.shape)
    else:
        logger.info("No reddit top post retieved. No response available")

    # Store the top 100 Reddit posts file to S3 bucket
    fileName = 'reddit/'+timestr+'.'+file_format+'.gz'
    Dataframe_to_s3(s3_resource, df_RedditTopPost, bucketName, fileName, file_format)
    