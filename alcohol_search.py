import tweepy
import twitter_credentials
from tweepy import Stream
from tweepy.streaming import StreamListener
import pandas as pd
import json
import numpy as np

class InformationChecker():
    def checkValidation(self,tweet):
        if tweet['place'] != None:
            #print("CountryCode: "+tweet['place']['country_code'])
            return True
        else:
            return False



class TwitterAuthenticator():

    def authenticate_twitter_app(self):
        auth = tweepy.OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
        auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
        return auth

class TwitterSearcher():

    def __init__(self):
        self.twitter_autenticator = TwitterAuthenticator()


    def search_tweets_(self, query, language, geolocation,count,untildate):

        api = tweepy.API( self.twitter_autenticator.authenticate_twitter_app(),wait_on_rate_limit=True)
        tweetCounter = 0
        search_results = []
        searchedTweets=0

        check = InformationChecker();

        file = open("search_results.txt", "w")

        if not geolocation:
            print("Start sreaching without geo boundary")
            for tweet in tweepy.Cursor(api.search, q=query,lang="en").items(count):
                #print(tweet.user.screen_name, "Tweeted:", tweet.text, "AT: ",tweet.created_at)
                tweetToWrite = tweet._json
                searchedTweets=searchedTweets+1
                if(searchedTweets%100==0):
                    print(str(searchedTweets)+" still investigated -> "+str(tweetCounter)+ " are applicable")
                if(check.checkValidation(tweetToWrite)==True):
                    tweetCounter = tweetCounter + 1
                    file.write(json.dumps(tweetToWrite) + '\n')


        else:
            print("Start sreaching with geo boundary")
            for tweet in tweepy.Cursor(api.search, q=query, geocode=geolocation, lang="en").items(count):
                #print(tweet.user.screen_name, "Tweeted:", tweet.text, "AT: ",tweet.created_at)
                tweetToWrite = tweet._json
                searchedTweets = searchedTweets + 1
                if (searchedTweets%100 == 0):
                    print(str(searchedTweets) + " still investigated -> " + str(tweetCounter) + " are applicable")
                if (check.checkValidation(tweetToWrite) == True):
                    tweetCounter = tweetCounter + 1
                    file.write(json.dumps(tweetToWrite) + '\n')

        print("Now "+str(tweetCounter)+ " tweets are collected from ["+str(count)+"] -> Write to JSON file")
        with open('search_results.json', 'w') as f:
            json.dump(search_results, f)

        print("Sreaching tweets finished")


if __name__ == '__main__':
    query = "alcohol OR beer OR wine OR drunk OR (drinking AND alcohol)"
    language="en"
    geolocation=""
    untildate="2019-01-01"
    count=10000

    searcher = TwitterSearcher()
    searcher.search_tweets_(query,language,geolocation,count,untildate)