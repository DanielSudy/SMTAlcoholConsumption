import time
import tweepy

from twitter_auth import authenticate_twitter_app
from db_stuff import UserDB

if __name__ == '__main__':
    db = UserDB("main_alc.db")
    
    twitter_auth = authenticate_twitter_app()
    twitter_api = tweepy.API(twitter_auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    
    user_ids = db.get_distinct_users()
    for i in range(0, int(len(user_ids)/2)):
        id = user_ids[i]
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
                sleep = True
                if e.response.status_code == 401:
                    # cannot crawl timeline of current user
                    finished = True
                    sleep = False
                #else undo changes and sleep
                try:
                    db.undo_changes()
                except:
                    print("Could not undo changes - probably no changes made!")
                if sleep:
                    time.sleep(600)
            except Exception as e:
                print(e)
        db.save_changes()