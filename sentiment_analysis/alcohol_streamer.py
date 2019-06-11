import tweepy
from tweepy import Stream
from tweepy.streaming import StreamListener

from twitter_auth import authenticate_twitter_app
from db_stuff import UserDB

class MyStreamListener(tweepy.StreamListener):
    """
    Twitter listener, collects streaming tweets and output to a file
    """

    def __init__(self, output_file="alc_tweets.db", max_tweets=1000):
        super(MyStreamListener, self).__init__()
        self.max_tweets = max_tweets
        self.num_tweets = 0
        self.good_tweets = 0
        self.db = UserDB(output_file)

    def on_status(self, status):
        #print(status.text)
        tweet = status._json
        self.num_tweets=self.num_tweets+1
        
        text = ""
        
        # catching extended tweets (only way i found to do this with streaming API)
        try:
            text = status.extended_tweet['full_text']
        except:
            text = status.text

        if status.place != None:
            print("Inserting tweet of length: " + str(len(text)))
            print("Text: " + text)
            print("Country code: " + status.place.country_code)
            self.good_tweets=self.good_tweets+1
            self.db.insert_tweet(int(status.user.id_str), text, status.place.country_code)
            self.db.save_changes()

        # Stops streaming when it reaches the limit
        if self.num_tweets <= self.max_tweets:
            if self.num_tweets % 100 == 0:  # just to see some progress...
                print(str(self.num_tweets) + " collected -> " + str(self.good_tweets) + " are applicable")
            return True
        else:
            return False

    def on_error(self, status):
        print(status)
        return False
        
    def __del__(self):
        pass


if __name__ == '__main__':

    print("Run Listener for crawling twitter data")

    #Define search content
    key_words =["alcohol,beer,wine,drunk,drinking alcohol,party alcohol"]


    l = MyStreamListener(max_tweets=100000)

    # Create you Stream object with authentication
    auth = authenticate_twitter_app()
    stream = tweepy.Stream(auth=auth, listener=l)

    # Filter Twitter Streams to capture data by the keywords:
    stream.filter(track=key_words,languages=['en'])

# try out db stuff
    
