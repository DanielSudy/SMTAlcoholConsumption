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
import matplotlib.pyplot as plt


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


            query = "INSERT INTO data_scr_tweets(CreationDate,AccountCreated,TweetID,UserName,UserID,PorfilPictureURL," \
                    "Gender,UserFollowerCount,UserFriendCount,UsedDevice,Tweet,ReplayID,CountryName," \
                    "CountryCode,Location,GeoLong,GeoLat,Sentiment) " \
                    "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            count_row = pdstruct.shape[0]
            for i in range(0, count_row):

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

    def openStatement(self, query):
        try:
            mycursor = self.mydb.cursor()
            mycursor.execute(query)
            self.mydb.commit()
            return mycursor.rowcount
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
    faces = recognition.MSAzureFaceRecogntion()
    suit = workingsuit.WorkingSuit()
    sql = MySQLWriter()
    genderDedector = gender.Detector(case_sensitive=False)


    #Control Variables

    do_general_tweet_db_stoarage=False #Just for initial
    do_face_reco = True
    do_graphical_analysis = True
    do_user_db_exctraction = True
    do_country_db_extraction = True
    do_tweets_per_country_db_extraction = True
    do_genearte_import_file=True
    do_generate_continent_statistic=True
    do_generate_age_classes = True



    # Write the dataframe to MySQL database
    # ===================================================================================================================
    if(do_general_tweet_db_stoarage==True):
        pdstruct = jHandler.setupDataStrcuture("livetweets_abImport.txt")
        print("There are " + str(pdstruct.shape[0]) + " rows in dataframe")
        textAnalyzer.start(pdstruct)
        storage.storeToDatabase(sql,pdstruct)


    #Create user statistics table
    # ===================================================================================================================
    if(do_user_db_exctraction==True):
        print("========Do User Extraction")
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

    # Create user countries
    # ===================================================================================================================
    if(do_country_db_extraction == True):
        print("========Do User Country Extraction")
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


    # Create country statistics table
    # ===================================================================================================================
    if (do_tweets_per_country_db_extraction == True):
        print("========Do Country Statistics")
        sqlCountryStatistic ="CREATE TABLE IF NOT EXISTS `data_scr_country_statistic` (`CountryCode` varchar(20) NOT NULL," \
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
            args = (str(row[0]), str(row[1]), str(row[2]), str(row[3]), str(row[4]))
            res = sql.writeStatement(query, args)
            if (res != -1):
                resCount = resCount + res
        print(str(resCount) + " countries are added to country statistic table")

    # Create continent statistics table
    # ===================================================================================================================
    if (do_generate_continent_statistic == True):
        print("========Do Continent Statistics")
        sqlCountryStatistic = "CREATE TABLE IF NOT EXISTS `data_scr_continent_statistic` (`Continent` varchar(100) NOT NULL," \
                              "`PosSentiment` int(11) NOT NULL," \
                              "`NegSentiment` int(11) NOT NULL," \
                              "`TweetCount` int(11) NOT NULL,PRIMARY KEY (`Continent`)) ENGINE=InnoDB DEFAULT CHARSET=latin1"
        sql.createTableStatement(sqlCountryStatistic)

        sql.deleteTableContent("data_scr_continent_statistic")

        select = "SELECT CountryCode, PosSentiment, NegSentiment, TweetCount from data_scr_country_statistic order by CountryCode asc"
        result = sql.selectStatement(select)
        contients = {}
        resCount = 0
        valid=False
        for row in result:
            if (row[0] != ""):
                valid,continent = suit.getContinetalInfo(str(row[0]))
                if (valid):
                    if continent in contients:
                        contients[continent][0] += row[1]  # Pos
                        contients[continent][1] += row[2]  # Neg
                        contients[continent][2] += row[3]  # Count
                    else:
                        statistic = [0, 0, 0]
                        statistic[0] = row[1]
                        statistic[1] = row[2]
                        statistic[2] = row[3]
                        contients[continent] = statistic
                else:
                    print("CountryCode "+row[0]+" is unknown")

        print(contients)

        query = "INSERT INTO data_scr_continent_statistic (Continent, PosSentiment, NegSentiment, TweetCount) VALUES (%s,%s,%s,%s)" \
                " ON DUPLICATE KEY UPDATE PosSentiment=VALUES(PosSentiment),NegSentiment=VALUES(NegSentiment),TweetCount=VALUES(TweetCount)"

        for key in contients:
            # print(key+" corresponds to ", contients[key][0])
            args = (key, str(contients[key][0]), str(contients[key][1]), str(contients[key][2]))
            res = sql.writeStatement(query, args)

    #Do the face recognition
    # ===================================================================================================================
    if(do_face_reco==True):
        print("========Do Face Recognition")
        result = sql.selectStatement("Select UserID,ProfilPictureURL,NGender from `data_scr_userinfo` where FAge=0")
        NotIdentified=0
        counter=0
        normalizedUrl=""
        rowaffected=0
        MeanGender=""
        age=1
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
                if(faceframe['age'].values[0]==0.0):
                    age=1
                else:
                    age=faceframe['age'].values[0]
                rowaffected = sql.updateStatement("UPDATE `data_scr_userinfo` SET FGender='"+str(faceframe['gender'].values[0])+"', FAge='"+str(age)+"', MeanGender='"+MeanGender+"' where UserID=" + str(row[0]))

            else:
                print("Status: " + str(valid))
                NotIdentified=sql.updateStatement("UPDATE `data_scr_userinfo` SET FAge=-1, MeanGender='"+str( suit.getNormalizedGenderFromName(str(row[2])))+"' where UserID=" + str(row[0]))
                print("Result of NID Update: "+str(NotIdentified))
                rowaffected=0
            print("Affected: "+str(rowaffected))
            print(str(counter)+" users are prcoessed out of: "+str(len(result)))
            print("================================================================================================================================")
            counter=counter+1

    # Create age classes out of user statistics
    # ===================================================================================================================
    if(do_generate_age_classes==True):
        print("========Do Create Age Classes")
        sqlAgeClassStatistic = "CREATE TABLE IF NOT EXISTS `data_scr_age_classes` (`ClassName` varchar(100) NOT NULL," \
                              "`MaleCnt` int(11) NOT NULL," \
                              "`FemaleCnt` int(11) NOT NULL," \
                              "`OverallCnt` int(11) NOT NULL,PRIMARY KEY (`ClassName`)) ENGINE=InnoDB DEFAULT CHARSET=latin1"
        sql.createTableStatement(sqlAgeClassStatistic)

        sql.deleteTableContent("data_scr_age_classes")
        select = "SELECT MeanGender, FAge, Tweets, UserID from data_scr_userinfo where FAge!='-1' and FAge!='0' order by UserID"
        result = sql.selectStatement(select)

        query = ""
        args = ""
        ageclasses = {}
        for row in result:
            ageclass = suit.getAgeClass(row[1])
            if ageclass in ageclasses:
                if(str(row[0])=="male"):
                    ageclasses[ageclass][0] += 1        # Male
                    ageclasses[ageclass][2] += 1        # Count
                else:
                    ageclasses[ageclass][1] += 1        # Female
                    ageclasses[ageclass][2] += 1        # Xount
            else:
                statistic = [0, 0, 0]
                if(str(row[0]) == "male"):
                    statistic[0] = 1                    #Male
                    statistic[2] = 1                    #Count
                else:
                    statistic[1] = 1                    # Female
                    statistic[2] = 1                    # Count
                ageclasses[ageclass] = statistic

        print(ageclasses)
        query = "INSERT INTO data_scr_age_classes (ClassName, MaleCnt, FemaleCnt, OverallCnt) VALUES (%s,%s,%s,%s)" \
                " ON DUPLICATE KEY UPDATE MaleCnt=VALUES(MaleCnt),FemaleCnt=VALUES(FemaleCnt),OverallCnt=VALUES(OverallCnt)"

        for key in ageclasses:
            # print(key+" corresponds to ", contients[key][0])
            args = (key, str(ageclasses[key][0]), str(ageclasses[key][1]), str(ageclasses[key][2]))
            res = sql.writeStatement(query, args)






    # Create import fiel for heatmapper
    # ===================================================================================================================
    if(do_genearte_import_file==True):
        print("========Do Heatmap File Creation")
        result = sql.selectStatement("SELECT CountryName,TweetCount,PosSentiment,NegSentiment FROM `data_scr_country_statistic` where CountryName!=''")
        if(result!=-1):
            jHandler.writeToCountryFile("Heatmapper File\countryMap.txt",result)


    #ANALYSIS AND GRAPHIC SECTION
    #===================================================================================================================

    if (do_graphical_analysis == True):
        print("========Do Grahpic Analysis")

        result=sql.selectStatement("SELECT MeanGender,count(MeanGender) AS GenderCount FROM data_scr_userinfo GROUP BY MeanGender")
        df = pd.DataFrame(result)

        graphics.showPieChart(df[1],df[0],"Gender Distribution of Users","GenderStatistic.png")
        graphics.showBarchart(df[0], df[1], "User", "Users per Gender Group", "UsersPerGenderGroup.png")

        result = sql.selectStatement("SELECT COUNT(u1.Rating) AS count, 'Negative' as type FROM data_scr_userinfo u1 WHERE u1.Rating<0 and u1.MeanGender='female' "
                                     "UNION ALL SELECT COUNT(u2.Rating) AS count, 'Positive'FROM data_scr_userinfo u2 WHERE u2.Rating>0 and u2.MeanGender='female'"
                                     " UNION ALL SELECT COUNT(u3.Rating) AS count, 'Neutral' FROM data_scr_userinfo u3 WHERE u3.Rating=0 and u3.MeanGender='female'")
        df = pd.DataFrame(result)
        #print(df)
        graphics.showPieChart(df[0], df[1], "Sentiment of Female Twitter User","FemnaleStatistic.png")

        result = sql.selectStatement("SELECT COUNT(u1.Rating) AS count, 'Negative' as type FROM data_scr_userinfo u1 WHERE u1.Rating<0 and u1.MeanGender='male' "
                                     "UNION ALL SELECT COUNT(u2.Rating) AS count, 'Positive'FROM data_scr_userinfo u2 WHERE u2.Rating>0 and u2.MeanGender='male'"
                                     " UNION ALL SELECT COUNT(u3.Rating) AS count, 'Neutral' FROM data_scr_userinfo u3 WHERE u3.Rating=0 and u3.MeanGender='male'")
        df = pd.DataFrame(result)
        graphics.showPieChart(df[0], df[1], "Sentiment of Male Twitter User","MaleStatistic.png")

        #print(df)

        result = sql.selectStatement("SELECT COUNT(u1.FAge) AS count, 'Classified' as type FROM data_scr_userinfo u1 WHERE u1.FAge!='-1' UNION ALL SELECT COUNT(u2.FAge) AS count, 'Unclassified'FROM data_scr_userinfo u2 WHERE u2.FAge='-1'")
        df = pd.DataFrame(result)
        graphics.showPieChart(df[0], df[1], "Age Classification of Users", "AgeClassificationStatistic.png")

        #print(df)

        result = sql.selectStatement("SELECT * FROM data_scr_age_classes")
        df = pd.DataFrame(result)
        graphics.showBarchart(df[0],df[1],"User","User per Age Class","AgeClassUserStatistic.png")

        result = sql.selectStatement("SELECT Continent, TweetCount from data_scr_continent_statistic where TweetCount>100")
        df = pd.DataFrame(result)
        graphics.showPieChart(df[1], df[0], "Continental Overview of Alcohol related Tweets", "ContinentOverviewTweetCount.png")

        result = sql.selectStatement("SELECT CountryName,PosSentiment,NegSentiment*-1 FROM `data_scr_country_statistic` order by TweetCount desc limit 15 ")
        df = pd.DataFrame(result)
        graphics.showDoubleBarchart(df[1],df[2],df[0],"Positive,","Negative","Sentiment per Country Top 15","SentimentCountry.png")
    print("Data management done...")