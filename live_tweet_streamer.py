import tweepy
import twitter_credentials
from tweepy import Stream
from tweepy.streaming import StreamListener
import pandas as pd
import json
import numpy as np
from pygal_maps_world.maps import World


from alcohol_search import TwitterAuthenticator

TRACKING_KEYWORDS = ['h']
OUTPUT_FILE = ""
TWEETS_TO_CAPTURE = 0

pd.set_option('display.width', 400)
pd.set_option('display.max_colwidth', -1)
pd.set_option('display.max_columns', 10)



class MyStreamListener(tweepy.StreamListener):
    """
    Twitter listener, collects streaming tweets and output to a file
    """

    def __init__(self, api=None):
        super(MyStreamListener, self).__init__()
        self.num_tweets = 0
        self.good_tweets = 0
        self.file = open(OUTPUT_FILE, "a+")

    def on_status(self, status):
        tweet = status._json
        self.num_tweets=self.num_tweets+1

        if status.place != None and status.coordinates != None:
            self.good_tweets=self.good_tweets+1
            self.file.write(json.dumps(tweet) + '\n')

        # Stops streaming when it reaches the limit
        if self.num_tweets <= TWEETS_TO_CAPTURE:
            if self.num_tweets % 100 == 0:  # just to see some progress...
                print(str(self.num_tweets) + " still investigated -> " + str(self.good_tweets) + " are applicable")
            return True
        else:
            return False
        self.file.close()

    def on_error(self, status):
        print(status)


if __name__ == '__main__':

    OUTPUT_FILE = "livetweets.txt"
    TWEETS_TO_CAPTURE = 1000000

    print("Run Listener for crawling twitter data")

    #Define search content
    key_words =["alcohol,beer,wine,drunk,drinking alcohol,party alcohol"]


    l = MyStreamListener()

    # Create you Stream object with authentication
    auth = TwitterAuthenticator().authenticate_twitter_app()
    stream = tweepy.Stream(auth, l)

    # Filter Twitter Streams to capture data by the keywords:
    stream.filter(track=key_words,languages=['en'])

