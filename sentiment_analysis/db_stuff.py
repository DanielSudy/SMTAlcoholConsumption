import os
import sqlite3

from collections import defaultdict

# this is used to create a dictionary of the query result
# see also: https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.row_factory
def query_to_dict(data):
    # col names are 7-tuples, the first ist the name of the col
    col_names = [name[0] for name in data.description]
    
    d = defaultdict(list)
    for row in data:
        for i in range(len(row)):
            d[col_names[i]].append(row[i])
    return d

class UserDB():

    conn = None
    c = None
    
    def __init__(self, db_file):
        if not os.path.isfile(db_file):
            self.conn = sqlite3.connect(db_file)
            self.c = self.conn.cursor()
            
            # i forgot to add the UNIQUE constraint in the beginning so we will not be using the
            # relation constraint in user_tweets. Because it only works with unique values?!
            
            self.c.execute('''CREATE TABLE tweets
                (user_id Int NOT NULL UNIQUE, content Varchar(240) NOT NULL, country Varchar(10))''')
            self.c.execute('''CREATE TABLE user_tweets
                (user_id Int NOT NULL,
                 content Varchar(240) NOT NULL)''')
            self.c.execute('''CREATE TABLE user_sentiment
                (user_id Int NOT NULL,
                timeline_cnt Int NOT NULL,
                alcohol_cnt Int NOT NULL,
                negative_tb_cnt Int NOT NULL,
                negative_af_cnt Int NOT NULL,
                positive_tb_cnt Int NOT NULL,
                positive_af_cnt Int NOT NULL,
                neutral_tb_cnt Int NOT NULL,
                neutral_af_cnt Int NOT NULL,
                powerful_tb_cnt Int NOT NULL,
                powerful_af_cnt Int NOT NULL,
                powerful_negative_tb_cnt Real NOT NULL,
                powerful_negative_af_cnt Int NOT NULL,
                powerful_positive_tb_cnt Real NOT NULL,
                powerful_positive_af_cnt Int NOT NULL,
                sentiment_tb_sum Real NOT NULL,
                sentiment_af_sum Int NOT NULL)''')
            self.conn.commit()
        else:
            self.conn = sqlite3.connect(db_file)
            self.c = self.conn.cursor()
            self.c.execute('''CREATE TABLE user_sentiment
                (user_id Int NOT NULL,
                timeline_cnt Int NOT NULL,
                alcohol_cnt Int NOT NULL,
                negative_tb_cnt Int NOT NULL,
                negative_af_cnt Int NOT NULL,
                positive_tb_cnt Int NOT NULL,
                positive_af_cnt Int NOT NULL,
                neutral_tb_cnt Int NOT NULL,
                neutral_af_cnt Int NOT NULL,
                powerful_tb_cnt Int NOT NULL,
                powerful_af_cnt Int NOT NULL,
                powerful_negative_tb_cnt Real NOT NULL,
                powerful_negative_af_cnt Int NOT NULL,
                powerful_positive_tb_cnt Real NOT NULL,
                powerful_positive_af_cnt Int NOT NULL,
                sentiment_tb_sum Real NOT NULL,
                sentiment_af_sum Int NOT NULL)''')
            self.conn.commit()

        # self.c.execute("PRAGMA foreign_keys = ON")
        self.conn.isolation_level = None # for undoing changes
        
    def insert_tweet(self, user_id, content, country):
        self.c.execute("INSERT INTO tweets(user_id, content, country) VALUES(?,?,?)", (user_id, content, country))
        
    def insert_user_tweet(self, user_id, content):
        self.c.execute("INSERT INTO user_tweets(user_id, content) VALUES(?,?)", (user_id, content))
        
    def insert_user_sentiment(self, 
        user_id,
        timeline_cnt,
        alcohol_cnt,
        negative_tb_cnt,
        negative_af_cnt,
        positive_tb_cnt,
        positive_af_cnt,
        neutral_tb_cnt,
        neutral_af_cnt,
        powerful_tb_cnt, 
        powerful_af_cnt,
        powerful_negative_tb_cnt,
        powerful_negative_af_cnt,
        powerful_positive_tb_cnt,
        powerful_positive_af_cnt,
        sentiment_tb_sum,
        sentiment_af_sum):
        
        self.c.execute('''INSERT INTO user_sentiment(
            user_id,
            timeline_cnt,
            alcohol_cnt,
            negative_tb_cnt,
            negative_af_cnt,
            positive_tb_cnt,
            positive_af_cnt,
            neutral_tb_cnt,
            neutral_af_cnt,
            powerful_tb_cnt, 
            powerful_af_cnt,
            powerful_negative_tb_cnt,
            powerful_negative_af_cnt,
            powerful_positive_tb_cnt,
            powerful_positive_af_cnt,
            sentiment_tb_sum,
            sentiment_af_sum)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
            (user_id,
            timeline_cnt,
            alcohol_cnt,
            negative_tb_cnt,
            negative_af_cnt,
            positive_tb_cnt,
            positive_af_cnt,
            neutral_tb_cnt,
            neutral_af_cnt,
            powerful_tb_cnt, 
            powerful_af_cnt,
            powerful_negative_tb_cnt,
            powerful_negative_af_cnt,
            powerful_positive_tb_cnt,
            powerful_positive_af_cnt,
            sentiment_tb_sum,
            sentiment_af_sum))
        
    def save_changes(self):
        self.conn.commit()
        
    def undo_changes(self):
        self.c.execute("ROLLBACK")
        
    def show_distinct_users(self):
        for row in self.c.execute("SELECT DISTINCT user_id FROM tweets ORDER BY user_id"):
            print(row[0])
        
    # watch out db queries return duples
    def get_distinct_users(self):
        user_ids = self.c.execute("SELECT DISTINCT user_id FROM tweets ORDER BY user_id")
        return [id[0] for id in user_ids]
        
    def get_distinct_timeline_users(self):
        user_ids =  self.c.execute("SELECT DISTINCT user_id FROM user_tweets ORDER BY user_id")
        return [id[0] for id in user_ids]
    
    def get_tweets_of_user(self, user_id):
        tweets = self.c.execute("SELECT content FROM user_tweets WHERE user_tweets.user_id=?", (user_id,))
        return [tweet[0] for tweet in tweets]
        
    # create a dict for easier access!
    def get_sentiment_data(self):
        col_names = self.c.execute("PRAGMA table_info(user_sentiment)")
        data = self.c.execute("SELECT * FROM user_sentiment WHERE alcohol_sum <= 100 AND timeline_size > 3000 AND timeline_size < 3500") # filter out spam
        return query_to_dict(data)
    
    def __del__(self):
        print("Closing connection to database...")
        self.conn.close()