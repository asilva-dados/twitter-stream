import tweepy
import os
from kafka import KafkaProducer
import logging

from commons.secret_manager import get_secret_key

"""API ACCESS KEYS"""
credentials = get_secret_key('TWITTER_SECRETS','us-east-2')

producer = KafkaProducer(bootstrap_servers='localhost:9092')
search_term = 'covid'
topic_name = 'twitter'


def twitterAuth():
    # create the authentication object
    authenticate = tweepy.OAuthHandler(credentials['CONSUMER_KEY'], credentials['CONSUMER_SECRET'])
    # set the access token and the access token secret
    authenticate.set_access_token(credentials['ACCESS_TOKEN'], credentials['ACCESS_TOKEN_SECRET'])
    # create the API object
    api = tweepy.API(authenticate, wait_on_rate_limit=True)
    #client = tweepy.Client(bearer_token=credentials['BEARER_TOKEN'])
    return api


class TweetListener(tweepy.StreamingClient):

    def on_data(self, raw_data):
        logging.info(raw_data)
        producer.send(topic_name, value=raw_data)
        return True

    def on_errors(self, status_code):
        if status_code == 420:
            return False

    def start_streaming_tweets(self, search_term):
        self.add_rules(tweepy.StreamRule(search_term))
        self.sample(tweet_fields=['author_id','conversation_id','created_at','id','in_reply_to_user_id','public_metrics','text'],
                    expansions='author_id', user_fields=['id','name','username','created_at'])         


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    twitter_stream = TweetListener(credentials['BEARER_TOKEN'])
    twitter_stream.start_streaming_tweets(search_term)