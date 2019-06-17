Sentiment analysis comprises:

- Livestreaming (alcohol_streamer.py)
Fetch alcohol tweets, store them to the given database.
Note that it is not recommended to use this livestreamer, for we streamed and searched for
alcohol messages beforehand with ../alcohol_search.py and ../live_tweet_streamer.py and
imported the desired attributes with (csv_to_sqlite.py the SQL table had been exported to csv before).

- Timeline Crawling (timeline_crawler.py)
Read out user IDs from given DB (table tweets), fetch timelines of these users and
store them to dedicated table (user_tweets) of same db

- Sentiment Analysis (sentiment_analysis.py)
Read out timelines of given DB, analyze sentiment with TextBlob analyzer and AFINN.
Count number of alcohol related tweets.
...
Store results in same DB in dedicated table (user_sentiment)

- Visualizing Results (plotter.py)
Read out results, perform filtering, averaging etc.
Then plot final results with matplotlib

- Database management (SQLite) is done in (db_stuff.py)

- Merging of timelines (user_tweet_merger.py)
As two of us had been fetching timelines simultanuously we had to merge them afterwards

- Importing the exported MySQL tables to the SQLite Data base (csv_to_sqlite.py)
As we used MySQL for our first analysis we had to import the desired data to our SQLite
Database. One can import the usr_src_info table as csv with this script to the given database.

---------------------------------------------------------------------------------------------------------------------
Please note that the first 4 Python scripts depend on the respective previous one in order to be able to be executed.
Also, note that the used database file is hardcoded in the corresponding Python scripts
However, we deliver our result database file (main_alc.db) which is already named appropriately, so everything should run
from scratch. As the timeline fetching is done with the previously fetched user IDs one should specify new IDs in the for-loop if needed.

Packages used:
tweepy
TextBlob
tweet-preprocessor 0.5.0 https://pypi.org/project/tweet-preprocessor/#description
AFINN https://github.com/fnielsen/afinn
numpy
matplotlib
