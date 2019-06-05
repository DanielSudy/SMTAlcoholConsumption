import tweepy
import re
import pandas as pd
import json
import numpy as np
import mysql.connector
import analyzing
from textblob import TextBlob, Word, Blobber
import gender_guesser.detector as gender
import alcohol_search
import face_recognition as recognition


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
        df['account_created'] = list(map(lambda tweet: tweet['user']['created_at']
        if tweet['user'] != None else '', tweets_data))
        df['id'] = list(map(lambda tweet: tweet['id'], tweets_data))
        df['user'] = list(map(lambda tweet: tweet['user']['name']
        if tweet['user'] != None else '', tweets_data))
        df['user_id'] = list(map(lambda tweet: tweet['user']['id']
        if tweet['user'] != None else '', tweets_data))
        df['user_profil_pic_url'] = list(map(lambda tweet: tweet['user']['profile_image_url_https']
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

class UserAnalysis():
    def searchUserByID(self,ID):
        counter=0
        tweet=""
        auth = alcohol_search.TwitterAuthenticator()
        api = tweepy.API(auth.authenticate_twitter_app())
        user = api.get_user(ID)
        print(user.name)
        for status in tweepy.Cursor(api.user_timeline, screen_name=user.screen_name, tweet_mode="extended",q="#gameofthrones").items(1000):
            counter=counter+1
            tweet = status.full_text.lower()
            if (counter % 100 == 0):
                print(str(counter)+" out of "+str(1000))
            if((tweet.find("birthday")==1) or (tweet.find("happy")==1) or (tweet.find("congratulation")==1)):
                print(status.full_text.lower())






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

class Storage():
        def storeToDatabase(self,sql,pdstruct):

            print("Start SQL processing...")
            print("=======================")

            # delte Databse
            sql.deleteTableContent("data_scr_tweets")


            query = "INSERT INTO data_scr_tweets(CreationDate,AccountCreated,TweetID,UserName,UserID,Gender,UserFollowerCount,UserFriendCount,UsedDevice,Tweet,ReplayID,CountryName,CountryCode,GeoLong,GeoLat,Sentiment) " \
                    "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            count_row = pdstruct.shape[0]
            for i in range(0, count_row):
                #print(genderDedector.get_gender(str(pdstruct['user'].values[i]).split(" ")[0]))
                #print(str(pdstruct['created'].values[i]))

                args = (
                str(pdstruct['created'].values[i]),str(pdstruct['account_created'].values[i]), str(pdstruct['id'].values[i]), str(pdstruct['user'].values[i]),str(pdstruct['user_id'].values[i]),
                genderDedector.get_gender(str(pdstruct['user'].values[i]).split(" ")[0]),
                str(pdstruct['user_follower_cnt'].values[i]), str(pdstruct['user_friend_cnt'].values[i]),
                str(pdstruct['source'].values[i]), str(pdstruct['text'].values[i]), str(pdstruct['replay'].values[i]),
                str(pdstruct['country'].values[i]), str(pdstruct['country_code'].values[i]),
                str(pdstruct['coordinates'].values[i]['coordinates'][0]),
                str(pdstruct['coordinates'].values[i]['coordinates'][1]), str(pdstruct['sentiment'].values[i]))
                sql.writeStatement(query, args)


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

    def deleteTableContent(self,table):
        mycursor = self.mydb.cursor()
        var = "DELETE FROM "+table
        mycursor.execute(var)
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
    storage = Storage()
    users = UserAnalysis()
    faces = recognition.MSAzureFaceRecogntion()
    #sql = MySQLWriter()

    genderDedector = gender.Detector(case_sensitive=False)


    #WFile to handle
    pdstruct = jHandler.setupDataStrcuture("search_results_1.txt")
    print("There are "+str(pdstruct.shape[0])+" rows in dataframe")
    #print(pdstruct.head(10))


    #Start Preporcessing some stuff
    #===================================================================================================================
    textAnalyzer.start(pdstruct)

    #result = sql.selectStatement("select distinct UserID from `data_scr_tweets`")
    #for row in result:
        #print(row[0])
        #users.searchUserByID(row[0])


    count_row = pdstruct.shape[0]
    for i in range(0, count_row):
        print(str(pdstruct['user_profil_pic_url'].values[i]))

    image_url = 'https://pbs.twimg.com/profile_images/360996042/IMG_7621_cropped_normal.jpg'
    #image_url = 'https://upload.wikimedia.org/wikipedia/commons/3/37/Dagestani_man_and_woman.jpg'
    valid,faceframe=faces.getFaceInfos(image_url)
    if(valid):
        print("Status: "+str(valid)+", id="+faceframe['face_id'].values[0]+", age="+str(faceframe['age'].values[0])+", gender="+str(faceframe['gender'].values[0]))
    else:
        print("Status: " + str(valid))
    #Write the dataframe to MySQL database

    #storage.storeToDatabase(sql,pdstruct)



    #ANALYSIS SECTION
    #Get data from mysql db and process it
    #Get sentiment order
    #select distinct CountryCode, CountryName, SUM(Sentiment)as Sentiment from `data_scr_tweets`  group by CountryCode order by Sentiment desc

    """
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
    """