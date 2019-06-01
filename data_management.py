import tweepy
import pandas as pd
import json
import numpy as np
import mysql.connector

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
    def start(self,text):
        print(text)

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

    def writeStatement(self):
        mycursor=self.mydb.cursor()
        sql="INSERT INTO data_scr_tweets (CreationDate, TweetID, UserName, CountryCode) VALUES(%s, %i, %s, %s)"
        val=("2019-06-01 00:00:00",100,"DSudy","AT")

        sql_select_Query = "select * from data_scr_tweets"
        cursor = self.mydb.cursor()
        cursor.execute(sql_select_Query)
        records = cursor.fetchall()
        print("Total number of rows in python_developers is - ", cursor.rowcount)
        print("Printing each row's column values i.e.  developer record")
        for row in records:
            print("CreationDtae = ", row[0], )
            print("TweetID = ", row[1])
            print("USerName  = ", row[2])
            print("CountryCOde  = ", row[3], "\n")
        cursor.close()



if __name__ == '__main__':

    print("Run Preprocessing steps")
    jHandler = HandleDataFormat()
    textAnalyzer = TextAnalysis()

    pdstruct = jHandler.setupDataStrcuture("search_results.txt")
    print(pdstruct.head(10))

    textAnalyzer.start(pdstruct['text'])
    sql = MySQLWriter()
    sql.writeStatement()


