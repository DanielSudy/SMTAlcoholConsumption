import numpy as np
import matplotlib.pyplot as plt

from db_stuff import UserDB

if __name__ == '__main__':
    db = UserDB("alc_tweets.db")
    data = db.get_sentiment_data()
    #print(data["user_id"])
    

    # Plot
    plt.scatter(data["alcohol_sum"], data["sentiment_af_percent"], alpha=0.5)
    plt.title('Sentiment of users posting about alcohol')
    plt.xlabel('#Alcohol posts')
    plt.ylabel('Average sentiment according to AFINN')
    plt.show()
    