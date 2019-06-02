import tweepy
import re
import pandas as pd
import json
import numpy as np
import mysql.connector
import analyzing
from textblob import TextBlob, Word, Blobber


pd.set_option('display.width', 400)
pd.set_option('display.max_colwidth', -1)
pd.set_option('display.max_columns', 10)


class HandleDataFormat():
    def setupDataStrcuture(self,filename):
        tweets_data = []
        print("Start HandleDataFormat")
        # Open connection to file
        with open(filename, "r") as tweets_file:
            # Read in tweets and store in list
            for line in tweets_file:
                tweet = json.loads(line)
                tweets_data.append(tweet)

        df = pd.DataFrame()
        df['created'] = list(map(lambda tweet: tweet['created_at'], tweets_data))
        df['id'] = list(map(lambda tweet: tweet['id'], tweets_data))
        df['user'] = list(map(lambda tweet: tweet['user']['name']
        if tweet['user'] != None else '', tweets_data))
        df['user_follower_cnt'] = list(map(lambda tweet: tweet['user']['followers_count']
        if tweet['user'] != None else '', tweets_data))
        df['user_friend_cnt'] = list(map(lambda tweet: tweet['user']['friends_count']
        if tweet['user'] != None else '', tweets_data))
        df['user_loc'] = list(map(lambda tweet: tweet['user']['location']
        if tweet['user'] != None else '', tweets_data))
        df['source'] = list(map(lambda tweet: tweet['source'], tweets_data))
        df['text'] = list(map(lambda tweet: tweet['text'], tweets_data))
        df['replay'] = list(map(lambda tweet: tweet['in_reply_to_status_id'], tweets_data))
        df['country'] = list(map(lambda tweet: tweet['place']['country']
        if tweet['place'] != None else '', tweets_data))
        df['country_code'] = list(map(lambda tweet: tweet['place']['country_code']
        if tweet['place'] != None else '', tweets_data))
        df['coordinates'] = list(map(lambda tweet: tweet['coordinates'], tweets_data))


        return df

class TextAnalysis():
    def cleanTweet(self,tweet):
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])| (\w+:\ / \ / \S+)", " ", tweet).split())

    def start(self,df):
        count_row = df.shape[0]
        sentiment= []
        tweet_text=""
        user_name=""
        print(str(count_row))

        for i in range(0, count_row):
            tweet_text=self.cleanTweet(df['text'].values[i])
            user_name=self.cleanTweet(df['user'].values[i])
            analysis = TextBlob(tweet_text)
            # set sentiment
            if analysis.sentiment.polarity > 0:
                sentiment.append(1)
            elif analysis.sentiment.polarity == 0:
                sentiment.append(0)
            else:
                sentiment.append(-1)
            df['text'].values[i] = tweet_text
            df['user'].values[i] = user_name
        df['sentiment']=sentiment

class NumericAnalysis():
        def getCountryStatistic(self,df):
            #print(df.country.value_counts())
            key = df.country_code.value_counts().keys().tolist()
            counts = df.country_code.value_counts().tolist()
            return key, counts



class MySQLWriter():
    def __init__(self):
        self.host="localhost"
        self.user="root"
        self.password="sudy"
        self.database="social_media_technology"

        self.mydb = mysql.connector.connect(
            host=self.host,
            user=self.user,
            passwd=self.password,
            database=self.database
        )

    def writeStatement(self,query,args):
        mycursor=self.mydb.cursor()
        """
        query = "INSERT INTO data_scr_tweets(CreationDate,TweetID,UserName,UserFollowerCount,UserFriendCount,UsedDevice,Tweet,ReplayID,CountryName,CountryCode,GeoLong,GeoLat) " \
                "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        args = ("2019-06-01 00:00:00", 99, "TEST", 0,1,"WEB","EIN TWWET",666,"ÖSTERREICH","AT",14.56,16.3)
        """

        mycursor.execute(query, args)
        self.mydb.commit()

    def selectStatement(self,query):
        mycursor = self.mydb.cursor()
        mycursor.execute(query)
        records = mycursor.fetchall()
        return records


if __name__ == '__main__':

    print("Run Preprocessing steps")
    jHandler = HandleDataFormat()
    textAnalyzer = TextAnalysis()
    numericAnalyzer = NumericAnalysis()
    graphics = analyzing.GraphicAnalyzer()

    pdstruct = jHandler.setupDataStrcuture("search_results.txt")

    print("There are "+str(pdstruct.shape[0])+" rows in dataframe")


    textAnalyzer.start(pdstruct)


    #print(pdstruct.head(50))

    #Write the dataframe to MySQL database
    print("Start SQL processing...")
    print("=======================")
    sql = MySQLWriter()
    query = "INSERT INTO data_scr_tweets(CreationDate,TweetID,UserName,UserFollowerCount,UserFriendCount,UsedDevice,Tweet,ReplayID,CountryName,CountryCode,GeoLong,GeoLat,Sentiment) " \
            "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    count_row = pdstruct.shape[0]
    for i in range(0, count_row):
        #print(str(pdstruct['text'].values[i]))

        args = (str(pdstruct['created'].values[i]),str(pdstruct['id'].values[i]),str(pdstruct['user'].values[i]),str(pdstruct['user_follower_cnt'].values[i]),str(pdstruct['user_friend_cnt'].values[i]),
                str(pdstruct['source'].values[i]),str(pdstruct['text'].values[i]),str(pdstruct['replay'].values[i]),str(pdstruct['country'].values[i]),str(pdstruct['country_code'].values[i]),
                str(pdstruct['coordinates'].values[i]['coordinates'][0]),str(pdstruct['coordinates'].values[i]['coordinates'][1]),str(pdstruct['sentiment'].values[i]))
        #sql.writeStatement(query, args)


    #Get sentiment order
    #select distinct CountryCode, CountryName, SUM(Sentiment)as Sentiment from `data_scr_tweets`  group by CountryCode order by Sentiment desc


    result=sql.selectStatement("select distinct CountryCode, CountryName, SUM(Sentiment)as Sentiment from `data_scr_tweets` where Sentiment=1 group by CountryCode order by Sentiment desc")
    sent_key=[]
    sent_val=[]
    for row in result:
        sent_key.append(row[0])
        sent_val.append(row[2])

    print(sent_key)
    graphics.showBarchart(sent_key, sent_val, "Positive Tweets about alcohol per Country")

    result = sql.selectStatement(
        "select distinct CountryCode, CountryName, SUM(Sentiment)*-1 as Sentiment from `data_scr_tweets` where Sentiment=-1 group by CountryCode order by Sentiment desc")
    sent_key = []
    sent_val = []
    for row in result:
        sent_key.append(row[0])
        sent_val.append(row[2])

    print(sent_key)
    graphics.showBarchart(sent_key, sent_val, "Negative Tweets about alcohol per Country")