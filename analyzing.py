import matplotlib.pyplot as plt; plt.rcdefaults()
import numpy as np
import matplotlib.pyplot as plt

class GraphicAnalyzer():
    def showBarchart(self,df_key, df_counts,title):

        objects = df_key
        y_pos = np.arange(len(objects))
        performance = df_counts

        plt.bar(y_pos, performance, align='center', alpha=0.5)
        plt.xticks(y_pos, objects)
        plt.ylabel('Tweets')
        plt.title(title)
        plt.show()