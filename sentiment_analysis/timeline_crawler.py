from db_stuff import UserDB

if __name__ == '__main__':
    db = UserDB("alc_tweets.sql")
    db.show_distinct_users()