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
import working_suit as workingsuit
import time
import datetime
from dateutil.parser import parse
from mysql.connector import errorcode



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
        df['text'] = list(map(lambda tweet: tweet['full_text'], tweets_data))
        df['replay'] = list(map(lambda tweet: tweet['in_reply_to_status_id'], tweets_data))
        df['country'] = list(map(lambda tweet: tweet['place']['country']
        if tweet['place'] != None else '', tweets_data))
        df['country_code'] = list(map(lambda tweet: tweet['place']['country_code']
        if tweet['place'] != None else '', tweets_data))
        df['coordinates'] = list(map(lambda tweet: tweet['coordinates'], tweets_data))


        return df

    def writeToCountryFile(self,filename,result):
        f = open(filename, 'w')
        f.write("Country\tCount\n")
        for row in result:
            #print(str(row[0])+","+str(row[1]))
            f.write(str(row[0])+"\t"+str(row[1])+"\n")

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

        for i in range(0, count_row):
            tweet_text=self.cleanTweet(df['text'].values[i])
            user_name=self.cleanTweet(df['user'].values[i])
            creation_date=parse(df['created'].values[i])
            account_creation=parse(df['account_created'].values[i])
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
            df['created'].values[i] = creation_date
            df['account_created'].values[i] = account_creation
        df['sentiment']=sentiment

class NumericAnalysis():
        def getCountryStatistic(self,df):
            #print(df.country.value_counts())
            key = df.country_code.value_counts().keys().tolist()
            counts = df.country_code.value_counts().tolist()
            return key, counts

class Storage():
        def storeToDatabase(self,sql,pdstruct):

            resultSql=0
            importCounter=0
            completeCounter=0
            print("Start SQL processing...")
            print("=======================")

            # delte Databse
            #sql.deleteTableContent("data_scr_tweets")


            query = "INSERT INTO data_scr_tweets(CreationDate,AccountCreated,TweetID,UserName,UserID,PorfilPictureURL," \
                    "Gender,UserFollowerCount,UserFriendCount,UsedDevice,Tweet,ReplayID,CountryName," \
                    "CountryCode,Location,GeoLong,GeoLat,Sentiment) " \
                    "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            count_row = pdstruct.shape[0]
            for i in range(0, count_row):
                #print(genderDedector.get_gender(str(pdstruct['user'].values[i]).split(" ")[0]))
                #print(str(pdstruct['user'].values[i]))

                args = (
                str(pdstruct['created'].values[i]),str(pdstruct['account_created'].values[i]), str(pdstruct['id'].values[i]), str(pdstruct['user'].values[i]),str(pdstruct['user_id'].values[i]),
                str(pdstruct['user_profil_pic_url'].values[i]),genderDedector.get_gender(str(pdstruct['user'].values[i]).split(" ")[0]),
                str(pdstruct['user_follower_cnt'].values[i]), str(pdstruct['user_friend_cnt'].values[i]),
                str(pdstruct['source'].values[i]), str(pdstruct['text'].values[i]), str(pdstruct['replay'].values[i]),
                str(pdstruct['country'].values[i]), str(pdstruct['country_code'].values[i]),str(pdstruct['user_loc'].values[i]),
                str(pdstruct['coordinates'].values[i]['coordinates'][0]),
                str(pdstruct['coordinates'].values[i]['coordinates'][1]), str(pdstruct['sentiment'].values[i]))
                resultSql=sql.writeStatement(query, args)
                completeCounter=completeCounter+1
                if(resultSql!=-1):
                    importCounter=importCounter+1

            print("Added "+str(importCounter)+" items to database from "+str(completeCounter)+" elements in text file")



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
        try:
            mycursor.execute(query, args)
            self.mydb.commit()
            return mycursor.rowcount
        except:
            return -1

    def deleteTableContent(self,table):
        mycursor = self.mydb.cursor()
        try:
            var = "DELETE FROM "+table
            mycursor.execute(var)
            self.mydb.commit()
        except:
            return -1

    def selectStatement(self,query):
        try:
            mycursor = self.mydb.cursor()
            mycursor.execute(query)
            records = mycursor.fetchall()
            return records
        except:
            return -1

    def updateStatement(self,statement):
        try:
            mycursor = self.mydb.cursor()
            mycursor.execute(statement)
            self.mydb.commit()
            return mycursor.rowcount
        except:
            return -1

    def createTableStatement(self,statement):
        try:
            mycursor = self.mydb.cursor()
            mycursor.execute(statement)
        except:
            return -1


if __name__ == '__main__':

    print("Run Preprocessing steps")
    jHandler = HandleDataFormat()
    textAnalyzer = TextAnalysis()
    numericAnalyzer = NumericAnalysis()
    graphics = analyzing.GraphicAnalyzer()
    storage = Storage()
    users = UserAnalysis()
    faces = recognition.MSAzureFaceRecogntion()
    suit = workingsuit.WorkingSuit()
    sql = MySQLWriter()
    genderDedector = gender.Detector(case_sensitive=False)


    #Control Variables

    do_general_tweet_db_stoarage=False
    do_face_reco = True
    do_graphical_analysis = False
    do_user_db_exctraction = False
    do_country_db_extraction = False
    do_tweets_per_country_db_extraction = False
    do_genearte_import_file=False


    #WFile to handle
    pdstruct = jHandler.setupDataStrcuture("search_results_5.txt")
    print("There are "+str(pdstruct.shape[0])+" rows in dataframe")
    #print(pdstruct.head(10))


    #Start Preporcessing some stuff
    #===================================================================================================================




    # Write the dataframe to MySQL database
    # ===================================================================================================================
    if(do_general_tweet_db_stoarage==True):
        textAnalyzer.start(pdstruct)
        storage.storeToDatabase(sql,pdstruct)

    if(do_user_db_exctraction==True):
        sqlUserTable="CREATE TABLE IF NOT EXISTS `data_scr_userinfo` (`UserID` int(100) NOT NULL," \
                 "`UserName` varchar(255) NOT NULL," \
                 "`NGender` varchar(50) NOT NULL," \
                 "`FGender` varchar(50) NOT NULL," \
                 "`MeanGender` varchar(50) NOT NULL," \
                 "`FAge` float NOT NULL," \
                 "`Tweets` int(100) NOT NULL," \
                 "`Rating` int(100) NOT NULL,"\
                 "`ProfilPictureURL` varchar(500) NOT NULL," \
                 "`AccountCreated` datetime NOT NULL,PRIMARY KEY (`UserID`)) ENGINE=InnoDB DEFAULT CHARSET=latin1"
        sql.createTableStatement(sqlUserTable)

        select="SELECT DISTINCT UserID, UserName, Gender, COUNT(UserName) as Tweets, SUM(Sentiment) as Rating, PorfilPictureURL, AccountCreated  FROM `data_scr_tweets` GROUP BY UserID"
        result = sql.selectStatement(select)
        resCount=0
        query = "INSERT INTO data_scr_userinfo(UserID,UserName,NGender,Tweets,Rating,ProfilPictureURL,AccountCreated) " \
                "VALUES(%s,%s,%s,%s,%s,%s,%s)"
        for row in result:
            args = (str(row[0]),str(row[1]),str(row[2]),str(row[3]),str(row[4]),str(row[5]),str(row[6]))
            res=sql.writeStatement(query, args)
            if(res !=-1):
                resCount=resCount+res
        print(str(resCount)+" users are added to user table")

    if(do_country_db_extraction == True):
        sqlUserCountries ="CREATE TABLE IF NOT EXISTS `data_scr_usercountries` (`UserID` int(100) NOT NULL," \
                              "`CountryName` varchar(255) NOT NULL," \
                              "`CountryCode` varchar(20) NOT NULL," \
                              "`Location` varchar(500) NOT NULL," \
                              "`GeoLong` float NOT NULL," \
                              "`GeoLat` float NOT NULL,PRIMARY KEY (`UserID`)) ENGINE=InnoDB DEFAULT CHARSET=latin1"
        sql.createTableStatement(sqlUserCountries)

        select="SELECT DISTINCT UserID, CountryName, CountryCode, Location, GeoLong, GeoLat FROM `data_scr_tweets` GROUP BY UserID"
        result = sql.selectStatement(select)
        resCount = 0
        query = "INSERT INTO data_scr_usercountries(UserID,CountryName,CountryCode,Location,GeoLong,GeoLat) " \
                "VALUES(%s,%s,%s,%s,%s,%s)"
        for row in result:
            args = (str(row[0]), str(row[1]), str(row[2]), str(row[3]), str(row[4]), str(row[5]))
            res = sql.writeStatement(query, args)
            if (res != -1):
                resCount = resCount + res
        print(str(resCount) + " users are added to country table")

    if (do_tweets_per_country_db_extraction == True):
        sqlCountryStatistic ="CREATE TABLE `data_scr_country_statistic` (`CountryCode` varchar(20) NOT NULL," \
                             "`CountryName` varchar(255) NOT NULL," \
                             "`PosSentiment` int(11) NOT NULL," \
                             "`NegSentiment` int(11) NOT NULL," \
                             "`TweetCount` int(11) NOT NULL,PRIMARY KEY (`CountryCode`)) ENGINE=InnoDB DEFAULT CHARSET=latin1"
        sql.createTableStatement(sqlCountryStatistic)

        select = "SELECT DISTINCT CountryCode, CountryName,COUNT(CountryCode) as TweetCount," \
                 " SUM(CASE WHEN Sentiment<0 THEN Sentiment ELSE 0 END) as NegSentiment," \
                 "SUM(CASE WHEN Sentiment>0 THEN Sentiment ELSE 0 END) as PosSentiment FROM `data_scr_tweets`GROUP BY CountryCode"
        result = sql.selectStatement(select)
        resCount = 0
        query = "INSERT INTO data_scr_country_statistic (CountryCode, CountryName, TweetCount,NegSentiment,PosSentiment) VALUES (%s,%s,%s,%s,%s)" \
                " ON DUPLICATE KEY UPDATE TweetCount=VALUES(TweetCount),PosSentiment=VALUES(PosSentiment),NegSentiment=VALUES(NegSentiment)"
        for row in result:
            #print(str(row[0])+"-"+str(row[1])+"-"+str(row[2])+"-"+str(row[3])+"-"+str(row[4]))
            args = (str(row[0]), str(row[1]), str(row[2]), str(row[3]), str(row[4]))
            res = sql.writeStatement(query, args)
            if (res != -1):
                resCount = resCount + res
        print(str(resCount) + " countries are added to country statistic table")


    #Do the face recognition
    # ===================================================================================================================
    if(do_face_reco==True):
        result = sql.selectStatement("Select UserID,ProfilPictureURL,NGender from `data_scr_userinfo` where FAge=0")
        NotIdentified=0
        counter=0
        normalizedUrl=""
        rowaffected=0
        MeanGender=""
        print("Users to search: "+str(len(result)))
        for row in result:
            if((counter % 19 == 0) and counter != 0):
                print("***********************************************************************")
                print("** HINT----->Wait 61 seconds from now: "+str(datetime.datetime.now()))
                print("***********************************************************************")
                time.sleep(61)

            print("Entry Start: " + str(counter))
            print("Date: " + str(datetime.datetime.now()))
            print(str(row[0])+", URL:"+str(row[1])+", Gender="+str(row[2]))
            normalizedUrl=str(suit.normalizeTwitterImageName(str(row[1])))
            print("Normalized URL: "+normalizedUrl)

            valid, faceframe = faces.getFaceInfos(normalizedUrl)
            if (valid):
                print("Status: " + str(valid) + ", id=" + faceframe['face_id'].values[0] + ", age=" + str(
                    faceframe['age'].values[0]) + ", gender=" + str(faceframe['gender'].values[0]))

                MeanGender=suit.getMeanGender(str(row[2]),str(faceframe['gender'].values[0]))
                print("MeanGender="+MeanGender)
                rowaffected = sql.updateStatement("UPDATE `data_scr_userinfo` SET FGender='"+str(faceframe['gender'].values[0])+"', FAge='"+str(faceframe['age'].values[0])+"', MeanGender='"+MeanGender+"' where UserID=" + str(row[0]))
            else:
                print("Status: " + str(valid))
                NotIdentified=sql.updateStatement("UPDATE `data_scr_userinfo` SET FAge=-1, MeanGender='"+str( suit.getNormalizedGenderFromName(str(row[2])))+"' where UserID=" + str(row[0]))
                print("Result of NID Update: "+str(NotIdentified))
                rowaffected=0
            print("Affected: "+str(rowaffected))
            print(str(counter)+" users are prcoessed out of: "+str(len(result)))
            print("================================================================================================================================")
            counter=counter+1

    #users.searchUserByID(row[0])

    #image_url = 'https://pbs.twimg.com/profile_images/1123335834009313283/urwaWKS6.jpg'
    #image_url = 'https://upload.wikimedia.org/wikipedia/commons/3/37/Dagestani_man_and_woman.jpg'
    #image_url = 'https://pbs.twimg.com/profile_images/1098476311830351872/pobLdxVF_normal.jpg'
    """
    print("A TEST")
    valid, faceframe = faces.getFaceInfos(suit.normalizeTwitterImageName(image_url))
    if (valid):
        print("Status: " + str(valid) + ", id=" + faceframe['face_id'].values[0] + ", age=" + str(faceframe['age'].values[0]) + ", gender=" + str(faceframe['gender'].values[0]))
    else:
        print("Status: " + str(valid))        
    """


    if(do_genearte_import_file==True):
        result = sql.selectStatement("SELECT CountryName,TweetCount,PosSentiment,NegSentiment FROM `data_scr_country_statistic` where CountryName!=''")
        if(result!=-1):
            jHandler.writeToCountryFile("countryMap.txt",result)

    #ANALYSIS SECTION
    # ===================================================================================================================
    #Get data from mysql db and process it
    #Get sentiment order
    #select distinct CountryCode, CountryName, SUM(Sentiment)as Sentiment from `data_scr_tweets`  group by CountryCode order by Sentiment desc

    if (do_graphical_analysis == True):

        result = sql.selectStatement(
            "select CountryCode, CountryName, PosSentiment from `data_scr_country_statistic` order by PosSentiment desc")
        sent_key = []
        sent_val = []
        for row in result:
            sent_key.append(row[0])
            sent_val.append(row[2])

        print(sent_key)
        graphics.showBarchart(sent_key, sent_val, "Positive Tweets about alcohol per Country")

    print("Data management done...")