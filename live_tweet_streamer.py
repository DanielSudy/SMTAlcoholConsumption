import tweepy
import twitter_credentials
from tweepy import Stream
from tweepy.streaming import StreamListener
import pandas as pd
import json
import numpy as np
from pygal_maps_world.maps import World


from tweepy_streamer import TwitterAuthenticator

TRACKING_KEYWORDS = ['h']
OUTPUT_FILE = "livestreamer.txt"
TWEETS_TO_CAPTURE = 50

pd.set_option('display.width', 400)
pd.set_option('display.max_colwidth', -1)
pd.set_option('display.max_columns', 10)

class HandleDataFormat():
    def init(self):
        tweets_data = []
        print("Start HandleDataFormat")
        # Open connection to file
        with open(OUTPUT_FILE, "r") as tweets_file:
            # Read in tweets and store in list
            for line in tweets_file:
                tweet = json.loads(line)
                tweets_data.append(tweet)

        df = pd.DataFrame()
        #df['text'] = list(map(lambda tweet: tweet['text'], tweets_data))

        df['location'] = list(map(lambda tweet: tweet['user']['location'], tweets_data))

        df['country_code'] = list(map(lambda tweet: tweet['place']['country_code']
        if tweet['place'] != None else '', tweets_data))

        df['long'] = list(map(lambda tweet: tweet['coordinates']['coordinates'][0]
        if tweet['coordinates'] != None else 'NaN', tweets_data))

        df['latt'] = list(map(lambda tweet: tweet['coordinates']['coordinates'][1]
        if tweet['coordinates'] != None else 'NaN', tweets_data))

        return df




class MyStreamListener(tweepy.StreamListener):
    """
    Twitter listener, collects streaming tweets and output to a file
    """

    def __init__(self, api=None):
        super(MyStreamListener, self).__init__()
        self.num_tweets = 0
        self.file = open(OUTPUT_FILE, "w")

    def on_status(self, status):
        tweet = status._json
        self.file.write(json.dumps(tweet) + '\n')

        if tweet['place'] != None and tweet['coordinates'] != None:
            print("No new data")
        else:
            print("Add data")
        #self.num_tweets += 1

        # Stops streaming when it reaches the limit
        if self.num_tweets <= TWEETS_TO_CAPTURE:

            if self.num_tweets % 10 == 0:  # just to see some progress...
                print('Numer of tweets captured so far: {}'.format(self.num_tweets))
            return True
        else:
            return False
        self.file.close()

    def on_error(self, status):
        print(status)


if __name__ == '__main__':

    print("Run Listener for crawling twitter data")

    #Define search content
    region_location = [-27.4,35.8,47.0,72.7]
    hash_tags = ["alcohol","beer","wine"," drinking alcohol","party"]

    capturing = True

    if capturing==True:
        l = MyStreamListener()
        # Create you Stream object with authentication
        auth = TwitterAuthenticator().authenticate_twitter_app()
        stream = tweepy.Stream(auth, l)

        # Filter Twitter Streams to capture data by the keywords:
        stream.filter(locations=region_location, languages=['en'], track=hash_tags)
    else:
        jHandler = HandleDataFormat()
        pdstruct = jHandler.init()
        print(pdstruct.head(100))


