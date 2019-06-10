import os
import sqlite3

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
                overall Real NOT NULL,
                alcohol_mentions Int,
                powerful_emotions Int)''')
            self.conn.commit()
        else:
            self.conn = sqlite3.connect(db_file)
            self.c = self.conn.cursor()
        # self.c.execute("PRAGMA foreign_keys = ON")
        self.conn.isolation_level = None # for undoing changes
        
    def insert_tweet(self, user_id, content, country):
        self.c.execute("INSERT INTO tweets(user_id, content, country) VALUES(?,?,?)", (user_id, content, country))
        
    def insert_user_tweet(self, user_id, content):
        self.c.execute("INSERT INTO user_tweets(user_id, content) VALUES(?,?)", (user_id, content))
        
    def insert_user_sentiment(self, user_id, overall, alcohol_mentions=None, powerful_emotions=None):
        self.c.execute("INSERT INTO user_sentiment(user_id, overall, alcohol_mentions, powerful_emotions) VALUES(?,?,?,?)",
            (user_id, overall, alcohol_mentions, powerful_emotions))
        
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
    
    def __del__(self):
        print("Closing connection to database...")
        self.conn.close()