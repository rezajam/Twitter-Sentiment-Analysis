# # """
# #     This Spark app connects to a script running on another (Docker) machine
# #     on port 9009 that provides a stream of raw tweets text. That stream is
# #     meant to be read and processed here, where top trending hashtags are
# #     identified. Both apps are designed to be run in Docker containers.

# #     To execute this in a Docker container, do:

# #         docker run -it -v $PWD:/app --link twitter:twitter eecsyorku/eecs4415

# #     and inside the docker:

# #         spark-submit spark_app.py

# #     For more instructions on how to run, refer to final tutorial 8 slides.

# #     Made for: EECS 4415 - Big Data Systems (York University EECS dept.)
# #     Modified by: Aditya and Reza
# #     Based on: https://www.toptal.com/apache/apache-spark-streaming-twitter
# #     Original author: Hanee' Medhat

import sys
import requests
import socket
import pyspark as ps
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import pyspark.streaming as pss
from pyspark.sql import Row, SQLContext
import re
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA
nltk.download('vader_lexicon')

# all the hashtags for the 5 topics
lakers = ['#lakers', '#lal', '#lbj', '#lakernation', '#lebron',
          '#lebronjames', '#kobe', '#losangeleslakers', '#lalakers', '#anthonydavis']
raptors = ['#raptors', '#torontoraptors', '#wethenorth', '#kylelowry', '#siakam',
           '#raptornation', '#nabchamps2019', '#freddy', '#steadyfreddy', '#raptorswin']
knicks = ['#nyknicks', '#knicks', '#knicksfan', '#knickstape', '#knicksnation',
          '#newyorkknicks', '#msg', '#nyknicksbasketball', '#rjbarrett', '#nyk']
warriors = ['#warriors', '#goldenstatewarriors', '#stephcurry', '#stephencurry', '#dubnation',
            '#gsw', '#warriorsnation', '#klaythompson', '#deangelorussell', '#warriorsground']
bulls = ['#chicagobulls', '#bulls', '#jordan', '#michaeljordan', '#bullsnation',
         '#bullsoutsiders', '#zachlavine', '#firegarpax', '#bullswin', '#scottiepippen']
hashtags_topic = lakers + raptors + knicks + warriors + bulls

# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# create the Streaming Context from spark context, interval size 2 seconds
ssc = StreamingContext(sc, 2)
# setting a checkpoint for RDD recovery (necessary for updateStateByKey)
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 9009
dataStream = ssc.socketTextStream("twitter", 9009)


# used for sentiment aanalysis
sia = SIA()


# do the sentiment analysis on the text and return 1, -1, 0 based on the analysis
def sentiment(text):
    # text = cleaning(text)
    text = re.sub(r'[^a-zA-Z#]', ' ', text)
    text = re.sub(r'[\s]', ' ', text)
    score = sia.polarity_scores(text)
    corp = score.get('compound')
    if (corp > 0):
        return 1.0
    elif corp < 0:
        return -1.0
    else:
        return 0.0

# the part responsible for basically the cleaning of the tweets


def checking(text):
    # calls the regular expression cleaning function to intially see (*)
    # text = cleaning(text)
    text = re.sub(r'[^a-zA-Z#]', ' ', text)
    text = re.sub(r'[\s]', ' ', text)
    for x in text.split(" "):
        if x.lower() in hashtags_topic:
            return True
    return False


# get return which topic the tweet is
def topic_det(text):

    # text = cleaning(text)
    text = re.sub(r'[^a-zA-Z#]', ' ', text)
    text = re.sub(r'[\s]', ' ', text)
    for x in text.split(" "):

        if x.lower() in lakers:

            return 'lakers'
        if x.lower() in raptors:

            return 'raptors'
        if x.lower() in knicks:

            return 'knicks'
        if x.lower() in warriors:
            return 'warriors'
        if x.lower() in bulls:

            return 'bulls'


# filter the words to tweets with the hashtags we are looking for
filtered_tweets = dataStream.filter(checking)

# map the topic and sentiment analysis
tweet_topic = filtered_tweets.map(lambda x: (topic_det(x), sentiment(x)))

# map the topic and number of times it is mentioned
tweet_topic2 = filtered_tweets.map(lambda x: (topic_det(x), 1))


# adding the count of each hashtag
def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0.0)


# get the total score of the sentiment of topic
tweet_totals = tweet_topic.updateStateByKey(aggregate_tags_count)

# get the total score of the total mentions of topic
tweet_totals2 = tweet_topic2.updateStateByKey(aggregate_tags_count)


# join thee two maps based on topic
tweet_join = tweet_totals.join(tweet_totals2)


def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']


# process a single time interval
def process_interval(time, rdd):
    # print a separator
    print("----------- %s -----------" % str(time))
    try:
        # for tag in rdd.collect():

        sql_context = get_sql_context_instance(rdd.context)
        # row_rdd = rdd.map(lambda w: Row(hashtag=w[0].encode("utf-8"), hashtag_count=w[1]))

        # map the RDD as a row with the setiment analysis divided by topic total mentions
        row_rdd = rdd.map(lambda w: Row(
            hashtag=w[0], avg_sen_analysis=(round((w[1][0]/w[1][1]), 2))))
        # row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=(w[1][0])))
        # row_rdd = rdd.map(lambda w: Row(hashtags=w[0]), hashtag_count=(float("{0:.2f}".format(w[1][0]/w[1][1]))))
        # print("before")
        # row_rdd = rdd.map(lambda w: Row(hashtags=w[0]), hashtag_count=float((w[1][0]/w[1][1])))

        hashtags_df = sql_context.createDataFrame(row_rdd)
        # dataframe as table
        hashtags_df.registerTempTable("hashtags")
        # print out all hashtags
        hashtag_counts_df = sql_context.sql(
            "select hashtag, avg_sen_analysis from hashtags")

        hashtag_counts_df.show()
        send_df_to_dashboard(hashtag_counts_df)
    except:
        e = sys.exc_info()[0]
        print("Error: {}".format(e))


def send_df_to_dashboard(df):
    # extract the hashtags from dataframe and convert them into array
    top_tags = [str(t.hashtag) for t in df.select("hashtag").collect()]
    # extract the counts from dataframe and convert them into array
    tags_count = [p.avg_sen_analysis for p in df.select(
        "avg_sen_analysis").collect()]

    # tags_count2 = [p.hashtag_count for p in df.select("hashtag_count2").collect()]
    # initialize and send the data through REST API
    url = 'http://server:5001/updateData'
    # url = 'http://localhost:5001/updateData'
    request_data = {'label': str(top_tags), 'data': str(tags_count)}
    response = requests.post(url, data=request_data)

# do this for every single interval


tweet_join.foreachRDD(process_interval)


# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()
