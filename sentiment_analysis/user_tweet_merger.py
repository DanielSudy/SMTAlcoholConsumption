from db_stuff import UserDB

db_dst = UserDB("main_alc.db")
db_src = UserDB("src.db")

tweets = db_src.get_user_tweets()
for tweet in tweets:
    db_dst.insert_user_tweet(tweet[0], tweet[1])