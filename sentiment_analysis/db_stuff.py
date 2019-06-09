import os
import sqlite3

class UserDB():

    conn = None
    c = None
    
    def __init__(self, db_file):
        if not os.path.isfile(db_file):
            self.conn = sqlite3.connect(db_file)
            self.c = self.conn.cursor()
            self.c.execute('''CREATE TABLE tweets
                (user_id Int NOT NULL, content Varchar(240) NOT NULL, country Varchar(10))''')
            self.conn.commit()
        else:
            self.conn = sqlite3.connect(db_file)
            self.c = self.conn.cursor()
            
            
    def insert(self, user_id, content, country):
        self.c.execute("INSERT INTO tweets(user_id, content, country) VALUES(?,?,?)", (user_id, content, country))
        self.conn.commit()
        
    def show_distinct_users(self):
        for row in self.c.execute("SELECT DISTINCT user_id FROM tweets ORDER BY user_id"):
            print(row[0])
            
    
    def __del__(self):
        print("Closing connection to database...")
        self.conn.close()