import numpy as np
import matplotlib.pyplot as plt

from db_stuff import UserDB

if __name__ == '__main__':
    db = UserDB("main_alc.db")
    data = db.get_sentiment_data()
    #print(data["user_id"])
    
    # average data
    averaged_x = []
    averaged_y = []
    
    for i in range(0,len(data["user_id"])):
        timeline_cnt = data["timeline_cnt"][i]
        averaged_x.append(data["alcohol_cnt"][i]/timeline_cnt)
        averaged_y.append(data["sentiment_tb_sum"][i]/timeline_cnt)
        
    # average
    x = np.arange(0.00125, 0.05, 0.01)
    interval = 0.007
    y = []
    for i in range(0,len(x)):
        sum = 0.0
        cnt = 0
        for j in range(0,len(data["user_id"])):
            if averaged_x[j] > x[i]-interval and averaged_x[j] <= x[i]+interval:
                sum = sum + averaged_y[j]
                cnt = cnt + 1
        if cnt > 0:
            y.append(sum/cnt)
        else:
            y.append(0)
    

    # Plot
    plt.scatter(averaged_x, averaged_y, alpha=0.5)
    plt.plot(x,y, color="red", linewidth=2.5, label="running average")
    plt.title('Neutral emotions of users posting about alcohol')
    plt.xlabel('Average alcohol posts')
    plt.ylabel('Average number of neutral emotions according to AFINN')
    plt.legend()
    plt.show()
    
    print("difference in percent: " + str((1-y[-1]/y[0])*100))
    