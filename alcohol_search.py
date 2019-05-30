import tweepy
import twitter_credentials
from tweepy import Stream
from tweepy.streaming import StreamListener
import pandas as pd
import json
import numpy as np


class TwitterAuthenticator():

    def authenticate_twitter_app(self):
        auth = tweepy.OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
        auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
        return auth

class TwitterSearcher():

    def __init__(self):
        self.twitter_autenticator = TwitterAuthenticator()


    def search_tweets_(self, query, language, geolocation,count):

        api = tweepy.API( self.twitter_autenticator.authenticate_twitter_app())
        tweetCounter = 0
        search_results = []

        file = open("search_results.txt", "w")

        if not geolocation:
            print("Start sreaching without geo boundary")
            for tweet in tweepy.Cursor(api.search, q=query,lang="en").items(count):
                tweetCounter=tweetCounter+1
                #print(tweet.user.screen_name, "Tweeted:", tweet.text, "AT: ",tweet.created_at)
                tweetToWrite = tweet._json
                file.write(json.dumps(tweetToWrite) + '\n')


        else:
            print("Start sreaching with geo boundary")
            for tweet in tweepy.Cursor(api.search, q=query, geocode=geolocation, lang="en").items(count):
                tweetCounter = tweetCounter + 1
                #print(tweet.user.screen_name, "Tweeted:", tweet.text, "AT: ",tweet.created_at)
                tweetToWrite = tweet._json
                file.write(json.dumps(tweetToWrite) + '\n')

        print("Now "+str(tweetCounter)+ " tweets are collected -> Write to JSON file")
        with open('search_results.json', 'w') as f:
            json.dump(search_results, f)

        print("Sreaching tweets finished")


if __name__ == '__main__':
    query = "alcohol OR beer OR wine OR (alcohol AND party) OR (drinking AND alcohol) OR drinking)"
    language="en"
    geolocation=""
    count=200

    searcher = TwitterSearcher()
    searcher.search_tweets_(query,language,geolocation,count)