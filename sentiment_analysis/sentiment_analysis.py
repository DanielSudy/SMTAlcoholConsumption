import preprocessor as p
from textblob import TextBlob
from afinn import Afinn

from db_stuff import UserDB

UPPER_EMOTION_THRESHOLD_TEXTBLOB = 0.5
LOWER_EMOTION_THRESHOLD_TEXTBLOB = -0.5

UPPER_EMOTION_THRESHOLD_AFINN = 4
LOWER_EMOTION_THRESHOLD_AFINN = -4

def is_powerful_emotion_textblob(score):
    return score >= UPPER_EMOTION_THRESHOLD_TEXTBLOB or score <= LOWER_EMOTION_THRESHOLD_TEXTBLOB
    
def is_powerful_emotion_afinn(score):
    return score >= UPPER_EMOTION_THRESHOLD_AFINN or score <= LOWER_EMOTION_THRESHOLD_AFINN

# note this is a very trivial method.. it may return false positives such as
# "I enjoy...", "joy" -> True
# "Bear", "ear" -> True
# If there is more time one could advance this but for our purpose it may be sufficient
def keyword_search(text, keyword):
    return keyword in text
    
def is_alcohol_content(tweet):
    return (keyword_search(tweet, "alcohol") or
       keyword_search(tweet, "beer") or
       keyword_search(tweet, "wine") or
       keyword_search(tweet, "drunk"))
        

if __name__ == '__main__':
    db = UserDB("alc_tweets.db")
    # allow emoticons
    p.set_options(p.OPT.EMOJI)
    af = Afinn(emoticons=True)
    
    #tweets = ["testing your pride, i drink alcohol!", "I'm a random message ;)", "Having insanely much fun right now!! :D", "I'm insane!!", "Reeed reeed wine"]
    #print(db.get_tweets_of_user(db.get_distinct_timeline_users()[1]))
    
    for user_id in db.get_distinct_timeline_users():
        alcohol_cnt = 0
        powerful_tb_cnt = 0
        powerful_af_cnt = 0
        sentiment_tb_sum = 0.0
        sentiment_af_sum = 0.0
        
        tweets = db.get_tweets_of_user(user_id)
        tweet_cnt = len(tweets)
        for tweet in tweets:
            clean_tweet = p.clean(tweet)
            
            tb_score = TextBlob(clean_tweet).sentiment[0]
            af_score = af.score(clean_tweet)
            
            #print(clean_tweet)
            #print(af_score)
            
            powerful_tb_cnt = powerful_tb_cnt + int(is_powerful_emotion_textblob(tb_score))
            powerful_af_cnt = powerful_af_cnt + int(is_powerful_emotion_afinn(af_score))
                
            sentiment_tb_sum = sentiment_tb_sum + tb_score 
            sentiment_af_sum = sentiment_af_sum + af_score
            
            alcohol_cnt = alcohol_cnt + int(is_alcohol_content(tweet))
            
        alcohol_percent = alcohol_cnt / float(tweet_cnt)
        powerful_tb_percent = powerful_tb_cnt / float(tweet_cnt)
        powerful_af_percent = powerful_af_cnt / float(tweet_cnt)
        sentiment_tb_percent = sentiment_tb_sum / float(tweet_cnt)
        sentiment_af_percent = sentiment_af_sum / float(tweet_cnt)
        
        db.insert_user_sentiment(user_id, tweet_cnt, alcohol_cnt,
            powerful_tb_cnt, powerful_af_cnt, sentiment_tb_sum, sentiment_af_sum, alcohol_percent,
            powerful_tb_percent, powerful_af_percent, sentiment_tb_percent, sentiment_af_percent)
        db.save_changes()
        
        print("alcohol tweets: " + str(alcohol_cnt))
        print("textblob strong emotions: " + str(powerful_tb_cnt))
        print("afinn strong emotions: " + str(powerful_af_cnt))
        print("textblob overall sentiment: " + str(sentiment_tb_sum))
        print("afinn overall sentiment: " + str(sentiment_af_sum))
        
        print("alcohol tweets percentage: " + str(alcohol_percent))
        print("textblob strong emotions (percentage): " + str(powerful_tb_percent))
        print("afinn strong emotions (percentage): " + str(powerful_af_percent))
        print("textblob overall sentiment (percentage): " + str(sentiment_tb_percent))
        print("afinn overall sentiment (percentage): " + str(sentiment_af_percent))
    
