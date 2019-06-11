import time
import tweepy

from twitter_auth import authenticate_twitter_app
from db_stuff import UserDB

if __name__ == '__main__':
    db = UserDB("alc_tweets.db")
    
    twitter_auth = authenticate_twitter_app()
    twitter_api = tweepy.API(twitter_auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    
    for id in db.get_distinct_users():
        if (id >= 34829917):
            print("Searching for user id: " + str(id))
            finished = False
            # catching twitter api errors
            while not finished:
                try:
                    for status in tweepy.Cursor(twitter_api.user_timeline, user_id=id, tweet_mode='extended').items():
                        db.insert_user_tweet(id, status.full_text)
                    finished = True
                except tweepy.TweepError as e:
                    print(e)
                    if e.response.status == 401:
                        # cannot crawl timeline of current user
                        finished = True
                    try:
                        db.undo_changes()
                    except:
                        print("Could not undo changes - probably no changes made!")
                    time.sleep(600)
                except Exception as e:
                    print(e)
            db.save_changes()