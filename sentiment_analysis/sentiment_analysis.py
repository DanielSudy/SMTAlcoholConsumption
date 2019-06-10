import preprocessor as p
from textblob import TextBlob
from afinn import Afinn

if __name__ == '__main__':
    # allow emoticons
    p.set_options(p.OPT.EMOJI)
    af = Afinn(emoticons=True)
    
    tweets = ["testing your pride!", "I'm a random message ;)", "I drink so much alcohol all the time dude!", "I feel so depressed!"]
    for tweet in tweets:
        clean_tweet = p.clean(tweet)
        print(clean_tweet)
        txtb = TextBlob(clean_tweet)
        print("TextBlob analysis: " + str(txtb.sentiment))
        print("AFINN analysis: " + str(af.score(clean_tweet)))
