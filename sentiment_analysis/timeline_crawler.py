import time
import tweepy

from twitter_auth import authenticate_twitter_app
from db_stuff import UserDB

if __name__ == '__main__':
    db = UserDB("alc_tweets.sql")
    
    twitter_auth = authenticate_twitter_app()
    twitter_api = tweepy.API(twitter_auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    
    # watch out db queries return tuples!
    for id in db.get_distinct_users():
        if (id >= 14938090):
            print("Searching for user id: " + str(id))
            finished = False
            # catching twitter api errors
            while not finished:
                try:
                    for status in tweepy.Cursor(twitter_api.user_timeline, user_id=id, tweet_mode='extended').items():
                        db.insert_user_tweet(id, status.full_text)
                    finished = True
                except Exception as e:
                    print(e)
                    try:
                        db.undo_changes()
                    except:
                        print("Could not rollback - there were no changes!")
                    time.sleep(600)
            db.save_changes()