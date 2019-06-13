import numpy as np
import matplotlib.pyplot as plt

from db_stuff import UserDB

if __name__ == '__main__':
    db = UserDB("alc_tweets.db")
    data = db.get_sentiment_data()
    #print(data["user_id"])
    
    # average data
    averaged_x = []
    averaged_y = []
    
    for i in range(0,len(data["user_id"])):
        timeline_cnt = data["timeline_cnt"][i]
        averaged_x.append(data["alcohol_cnt"][i]/timeline_cnt)
        averaged_y.append(data["sentiment_tb_sum"][i]/timeline_cnt)
    

    # Plot
    plt.scatter(averaged_x, averaged_y, alpha=0.5)
    plt.title('Sentiment of users posting about alcohol')
    plt.xlabel('Average alcohol posts')
    plt.ylabel('Average sentiment according to TextBlob')
    plt.show()
    