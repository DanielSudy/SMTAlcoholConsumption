import preprocessor as p
from textblob import TextBlob
from afinn import Afinn

from db_stuff import UserDB

UPPER_EMOTION_THRESHOLD_TB = 0.6
LOWER_EMOTION_THRESHOLD_TB = -0.6

UPPER_EMOTION_THRESHOLD_AF = 5
LOWER_EMOTION_THRESHOLD_AF = -5

def is_powerful_negative_tb(score):
    return score <= LOWER_EMOTION_THRESHOLD_TB
    
def is_powerful_negative_af(score):
    return score <= LOWER_EMOTION_THRESHOLD_AF
    
def is_powerful_positive_tb(score):
    return score >= UPPER_EMOTION_THRESHOLD_TB
    
def is_powerful_positive_af(score):
    return score >= UPPER_EMOTION_THRESHOLD_AF

def is_powerful_emotion_tb(score):
    return score >= UPPER_EMOTION_THRESHOLD_TB or score <= LOWER_EMOTION_THRESHOLD_TB
    
def is_powerful_emotion_af(score):
    return score >= UPPER_EMOTION_THRESHOLD_AF or score <= LOWER_EMOTION_THRESHOLD_AF

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
   
        negative_tb_cnt = 0
        negative_af_cnt = 0
        positive_tb_cnt = 0
        positive_af_cnt = 0
        neutral_tb_cnt = 0
        neutral_af_cnt = 0
        
        powerful_tb_cnt = 0
        powerful_af_cnt = 0
        
        powerful_negative_tb_cnt = 0
        powerful_negative_af_cnt = 0
        powerful_positive_tb_cnt = 0
        powerful_positive_af_cnt = 0
        
        sentiment_tb_sum = 0.0
        sentiment_af_sum = 0.0
        
        tweets = db.get_tweets_of_user(user_id)
        timeline_cnt = len(tweets)
        for tweet in tweets:
            clean_tweet = p.clean(tweet)
            
            tb_score = TextBlob(clean_tweet).sentiment[0]
            af_score = af.score(clean_tweet)
            
            #print(clean_tweet)
            #print(af_score)
            
            if tb_score < 0:
                negative_tb_cnt = negative_tb_cnt + 1
            elif tb_score > 0:
                positive_tb_cnt = positive_tb_cnt + 1
            else:
                neutral_tb_cnt = neutral_tb_cnt + 1
            
            if af_score < 0:
                negative_af_cnt = negative_af_cnt + 1
            elif af_score > 0:
                positive_af_cnt = positive_af_cnt + 1
            else:
                neutral_af_cnt = neutral_af_cnt + 1
            
            powerful_tb_cnt = powerful_tb_cnt + int(is_powerful_emotion_tb(tb_score))
            powerful_af_cnt = powerful_af_cnt + int(is_powerful_emotion_af(af_score))
            powerful_negative_tb_cnt = powerful_negative_tb_cnt + int(is_powerful_negative_tb(tb_score))
            powerful_negative_af_cnt = powerful_negative_af_cnt + int(is_powerful_negative_af(af_score))
            powerful_positive_tb_cnt = powerful_positive_tb_cnt + int(is_powerful_positive_tb(tb_score))
            powerful_positive_af_cnt = powerful_positive_af_cnt + int(is_powerful_positive_af(af_score))
                
            sentiment_tb_sum = sentiment_tb_sum + tb_score 
            sentiment_af_sum = sentiment_af_sum + af_score
            
            alcohol_cnt = alcohol_cnt + int(is_alcohol_content(tweet))
            
        '''
        alcohol_percent = alcohol_cnt / float(timeline_cnt)
        powerful_tb_percent = powerful_tb_cnt / float(timeline_cnt)
        powerful_af_percent = powerful_af_cnt / float(timeline_cnt)
        sentiment_tb_percent = sentiment_tb_sum / float(timeline_cnt)
        sentiment_af_percent = sentiment_af_sum / float(timeline_cnt)
        
        powerful_low_tb_percent = powerful_negative_tb_cnt / float(timeline_cnt)
        powerful_low_af_percent = powerful_negative_af_cnt / float(timeline_cnt)
        powerful_high_tb_percent = powerful_positive_tb_cnt / float(timeline_cnt)
        powerful_high_af_percent = powerful_positive_af_cnt / float(timeline_cnt)
        '''
        
        db.insert_user_sentiment(
            user_id,
            timeline_cnt,
            alcohol_cnt,
            negative_tb_cnt,
            negative_af_cnt,
            positive_tb_cnt,
            positive_af_cnt,
            neutral_tb_cnt,
            neutral_af_cnt,
            powerful_tb_cnt, 
            powerful_af_cnt,
            powerful_negative_tb_cnt,
            powerful_negative_af_cnt,
            powerful_positive_tb_cnt,
            powerful_positive_af_cnt,
            sentiment_tb_sum,
            sentiment_af_sum)
        db.save_changes()
        
        print("alcohol tweets: " + str(alcohol_cnt))
        
        print("negative_tb_cnt = " + str(negative_tb_cnt))
        print("negative_af_cnt = " + str(negative_af_cnt))
        print("positive_tb_cnt = " + str(positive_tb_cnt))
        print("positive_af_cnt = " + str(positive_af_cnt))
        print("neutral_tb_cnt = " + str(neutral_tb_cnt))
        print("neutral_af_cnt = " + str(neutral_af_cnt))
        
        print("textblob strong emotions: " + str(powerful_tb_cnt))
        print("afinn strong emotions: " + str(powerful_af_cnt))
        print("textblob overall sentiment: " + str(sentiment_tb_sum))
        print("afinn overall sentiment: " + str(sentiment_af_sum))
        print("textblob powerful negative cnt: " + str(powerful_negative_tb_cnt))
        print("afinn powerful negative cnt: " + str(powerful_negative_af_cnt))
        print("textblob powerful positive cnt: " + str(powerful_positive_tb_cnt))
        print("afinn powerful positive cnt: " + str(powerful_positive_af_cnt))
        
        '''
        print("alcohol tweets percentage: " + str(alcohol_percent))
        print("textblob strong emotions (percentage): " + str(powerful_tb_percent))
        print("afinn strong emotions (percentage): " + str(powerful_af_percent))
        print("textblob overall sentiment (percentage): " + str(sentiment_tb_percent))
        print("afinn overall sentiment (percentage): " + str(sentiment_af_percent))
        print("textblob powerful negative (percentage): " + str(powerful_low_tb_percent))
        print("afinn powerful negative (percentage): " + str(powerful_low_af_percent))
        print("textblob powerful positive (percentage): " + str(powerful_high_tb_percent))
        print("afinn powerful positive (percentage): " + str(powerful_high_af_percent))
        '''
        