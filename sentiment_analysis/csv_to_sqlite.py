'''
parts of this file originate from [https://realpython.com/python-csv/]
'''
import csv

from db_stuff import UserDB

db = UserDB("main_alc.db")

with open("data_scr_userinfo.csv") as csv_file:
    r = csv.reader(csv_file, delimiter=',')
    for row in r:
        db.insert_tweet(row[0], "", "") #only use user_id, its somewhat wasteful but whatever
db.save_changes()