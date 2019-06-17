import matplotlib.pyplot as plt; plt.rcdefaults()
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import rcParams
import matplotlib as cm
import datetime


fig_size = plt.rcParams["figure.figsize"]
fig_size[0] = 12
fig_size[1] = 8
plt.rcParams["figure.figsize"] = fig_size
imgFolder="Graphics/"

class GraphicAnalyzer():

    def showBarchart(self,df_key, df_counts,yLabel,title,filename):
        plt.clf()
        plt.gcf().subplots_adjust(bottom=0.15)
        objects = df_key
        y_pos = np.arange(len(objects))
        performance = df_counts

        plt.bar(y_pos, performance, align='center', alpha=0.5)
        plt.xticks(y_pos, objects,rotation='horizontal')
        plt.ylabel(yLabel)
        plt.title(title)
        #plt.show()

        plt.savefig(imgFolder + filename, block=True)

    def showDoubleBarchart(self,data1,data2,x,LabelD1,LabelD2,title,filename):
        plt.clf()
        plt.gcf().subplots_adjust(bottom=0.20)
        ind = np.arange(len(data1))
        width = 0.35
        plt.bar(ind, data1, width, label=LabelD1)
        plt.bar(ind + width, data2, width,
                label=LabelD2)
        plt.ylabel('Tweets')
        plt.title(title)

        plt.xticks(ind + width / 2, x,rotation='vertical',fontsize=10)
        plt.legend(loc='best')
        #plt.show()
        plt.savefig(imgFolder + filename, block=True)



    def showPieChart(self,data, lables,title,filename):
        plt.clf()
        plt.gcf().subplots_adjust(bottom=0.15)
        rcParams['axes.titlepad'] = 25
        colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#8c564b"]
        explode = [0.1] * len(data)
        fig1, ax1 = plt.subplots()
        ax1.pie(data, explode=explode, labels=lables, autopct='%1.1f%%',
                shadow=True, startangle=90)
        ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
        plt.title(title)
        #plt.show()
        plt.savefig(imgFolder+filename,block=True)


