from sys import stdout
import sys
import requests
import pandas as pd
from datetime import date, timedelta, time , datetime
import praw
from pandas.io.json import json_normalize
import numpy as np
import logging

# Define logging for the function
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
if logger.handlers:
    logger.handlers = []
stream_handler = logging.StreamHandler(stdout)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


class RedditExceptions(Exception):
    def __init__(self,message,code):
        super().__init__(message)
        self.code = code

class ExtractRedditPost(object):
    """
    This class will extract the top 100 posts from Reddit for the specified topic
    """
    def __init__(self, api): 
        self._api = api
        
    def createDf(self):
        self.SubRedditDf = pd.DataFrame(data = None)
        return self.SubRedditDf
    
    @property
    def reddit_top_posts(self):
        """
        Subreddit top 100 posts on topic.
        """
        try: 
            logger.info("Get top reddit posts")
            reddit = self._api._getClient()
            logger.info (reddit)
            subreddit = reddit.subreddit(self._api._subreddit_topic)
            logger.info (subreddit) 
            top_subreddit_posts = subreddit.top(limit=100)
            logger.info("The top reddit posts extracted")
            return top_subreddit_posts           
        except:
            raise RedditExceptions('No reddit posts found for the specified topic', 103)
    
    @property
    def format_reddit_posts(self):
        """
        Format the posts to readable form.
        """
        try: 
            logger.info("Format the top 100 reddit post for topic")
            top_posts = self.reddit_top_posts
            logger.info (top_posts) 
            top_posts_list = []
            for posts in top_posts:
                post = dict({'post_id':posts.id
                            ,'title':posts.title
                            ,'url':posts.url
                            ,'selftext':posts.selftext
                            ,'author':posts.author
                            ,'upvote_ratio':posts.upvote_ratio
                            ,'over_18':posts.over_18
                            ,'score':posts.score
                            ,'num_comments':posts.num_comments
                            ,'author_premium':posts.author_premium
                            ,'treatment_tag':posts.treatment_tags
                            ,'created':posts.created
                            })
                top_posts_list.append(post)
            
            df_top_post = json_normalize(top_posts_list)
            self.SubRedditDf = df_top_post 
            
            logger.info("Formatted top 100 reddit posts")           
            return self.SubRedditDf
        except:
            raise RedditExceptions('Formatting reddit posts failed', 104)  
    
class API(object):
    def __init__(self, **kwargs):
        """Creates a wrapper to perform API actions.
        Arguments
        client_id: client id for reddit
        client_secret: client secret for reddit
        Instances:
          .subreddit : reddit hot posts
        """ 
        
        client_id = kwargs.get('client_id')
        client_secret = kwargs.get('client_secret')
        subreddit_topic = kwargs.get('topic')
        
        if all(key is not None for key in (
                client_id, client_secret)):
            self._client_id = client_id
            self._client_secret = client_secret
        else:
            raise RedditExceptions('Please provide valid login credentials', 101)  
            
        if subreddit_topic != '' and pd.notnull(subreddit_topic) and subreddit_topic is not None :
            self._subreddit_topic = subreddit_topic
        else:
            raise RedditExceptions('Please provide a valid reddit topic', 102) 
            
        self._session = requests.Session()
        self._session.verify = False  
        
        logger.info(self._client_id)
        logger.info(self._client_secret) 
        
        self.subreddit = ExtractRedditPost(self)

    def _getClient(self):
        """Reddit client"""
        client = praw.Reddit(client_id=self._client_id, 
                             client_secret=self._client_secret, 
                             user_agent='Pelago_proj_DB',
                             requestor_kwargs={'session': self._session})
        return client
