import os, sys, json
import findspark
findspark.init()

from pyspark.sql import SQLContext, SparkSession
from pyspark.streaming import StreamingContext
from pyspark import SparkContext

from pyspark.sql.functions import col
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer, IndexToString
from pyspark.ml.classification import LogisticRegression, NaiveBayes
from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.evaluation import BinaryClassificationEvaluator,MulticlassClassificationEvaluator
from pyspark.ml import PipelineModel
from collections import namedtuple

# Hosted elasticache URL
import redis
key = sys.argv[1]
print('key in streamer:')
print(key)

def train_model(dataRdd = None ):
  if (dataRdd != None):
    print("**************************************************************************************************** Inside train model with new rdd")
    # regular expression tokenizer
    regexTokenizer = RegexTokenizer(inputCol="content", outputCol="words", pattern="\\W")

    # bag of words count
    countVectors = CountVectorizer(inputCol="words", outputCol="features", vocabSize=10000, minDF=5)

    # convert string labels to indexes
    label_stringIdx = StringIndexer(inputCol = "sentiment", outputCol = "label")

    nb = NaiveBayes(featuresCol="features", labelCol="label", smoothing=1.0, modelType="multinomial")

    # convert prediction to the predictedSentiment
    indexToLabels = IndexToString(inputCol = "prediction", outputCol = "predictedSentiment", labels=["bordem","love","relief", "fun", "hate", "neutral", "anger", "happiness", "surpirse","sadness","worry", "empty"])

    # Buidl spark pipeline
    pipeline = Pipeline(stages=[regexTokenizer, countVectors, label_stringIdx, nb, indexToLabels])

    # Fit the pipelin.
    pipelineFit = pipeline.fit(dataRDD)
    pipelineFit.fit(dataRdd)
    print("Workinggggggggggggggg")
    pipeLineFit.save("sentiment.model")
  else:  
    data = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('text_emotion.csv')
    #Drop unused columns
    drop_list = ['tweet_id']
    data = data.select([column for column in data.columns if column not in drop_list]) \
               .where(
                      (data['sentiment'] == 'empty') |
                      (data['sentiment'] == 'sadness') |
                      (data['sentiment'] == 'enthusiam') |
                      (data['sentiment'] == 'worry') |
                      (data['sentiment'] == 'surprise') |
                      (data['sentiment'] == 'love') |
                      (data['sentiment'] == 'hate') |
                      (data['sentiment'] == 'anger') |
                      (data['sentiment'] == 'neutral') |
                      (data['sentiment'] == 'relief') |
                      (data['sentiment'] == 'boredom') |
                      (data['sentiment'] == 'fun') |
                      (data['sentiment'] == 'happiness')) \
               .na.drop(thresh=3)

    data.show(5)

    data.groupBy("sentiment") \
        .count() \
        .orderBy(col("count").desc()) \
        .show()

    # set seed for reproducibility
    (trainingData, testData) = data.randomSplit([0.8, 0.2], seed = 100)
    print(trainingData)
    print("Training Dataset Count: " + str(trainingData.count()))
    print("Test Dataset Count: " + str(testData.count()))

    # regular expression tokenizer
    regexTokenizer = RegexTokenizer(inputCol="content", outputCol="words", pattern="\\W")

    # bag of words count
    countVectors = CountVectorizer(inputCol="words", outputCol="features", vocabSize=10000, minDF=5)

    # convert string labels to indexes
    label_stringIdx = StringIndexer(inputCol = "sentiment", outputCol = "label")

    nb = NaiveBayes(featuresCol="features", labelCol="label", smoothing=1.0, modelType="multinomial")

    # convert prediction to the predictedSentiment
    indexToLabels = IndexToString(inputCol = "prediction", outputCol = "predictedSentiment", labels=["bordem","love","relief", "fun", "hate", "neutral", "anger", "happiness", "surpirse","sadness","worry", "empty"])

    # Buidl spark pipeline
    pipeline = Pipeline(stages=[regexTokenizer, countVectors, label_stringIdx, nb, indexToLabels])

    # Fit the pipelin.
    pipelineFit = pipeline.fit(trainingData)
    predictions = pipelineFit.transform(testData)

    predictions.filter(predictions['prediction'] == 0) \
        .select("content","sentiment","predictedSentiment","probability","label","prediction") \
        .orderBy("probability", ascending=False) \
        .show(n = 10, truncate = 30)

    predictions.filter(predictions['prediction'] == 1) \
        .select("content","sentiment", "predictedSentiment","probability","label","prediction") \
        .orderBy("probability", ascending=False) \
        .show(n = 10, truncate = 30)

    predictions.filter(predictions['prediction'] == 2) \
        .select("content","sentiment","predictedSentiment","probability","label","prediction") \
        .orderBy("probability", ascending=False) \
        .show(n = 10, truncate = 30)

    predictions.filter(predictions['prediction'] == 3) \
        .select("content","sentiment","predictedSentiment","probability","label","prediction") \
        .orderBy("probability", ascending=False) \
        .show(n = 10, truncate = 30)

    predictions.filter(predictions['prediction'] == 4) \
        .select("content","sentiment","predictedSentiment", "probability","label","prediction") \
        .orderBy("probability", ascending=False) \
        .show(n = 10, truncate = 30)

    predictions.filter(predictions['prediction'] == 5) \
        .select("content","sentiment","predictedSentiment","probability","label","prediction") \
        .orderBy("probability", ascending=False) \
        .show(n = 10, truncate = 30)

    predictions.filter(predictions['prediction'] == 6) \
        .select("content","sentiment","predictedSentiment","probability","label","prediction") \
        .orderBy("probability", ascending=False) \
        .show(n = 10, truncate = 30)

    predictions.filter(predictions['prediction'] == 7) \
        .select("content","sentiment","predictedSentiment","probability","label","prediction") \
        .orderBy("probability", ascending=False) \
        .show(n = 10, truncate = 30)

    predictions.filter(predictions['prediction'] == 8) \
        .select("content","sentiment","predictedSentiment","probability","label","prediction") \
        .orderBy("probability", ascending=False) \
        .show(n = 10, truncate = 30)

    predictions.filter(predictions['prediction'] == 9) \
        .select("content","sentiment","predictedSentiment","probability","label","prediction") \
        .orderBy("probability", ascending=False) \
        .show(n = 10, truncate = 30)

    predictions.filter(predictions['prediction'] == 10) \
        .select("content","sentiment","predictedSentiment","probability","label","prediction") \
        .orderBy("probability", ascending=False) \
        .show(n = 10, truncate = 30)

    predictions.filter(predictions['prediction'] == 11) \
        .select("content","sentiment","predictedSentiment","probability","label","prediction") \
        .orderBy("probability", ascending=False) \
        .show(n = 10, truncate = 30)


    # Retrive F1 accuracy score
    evaluator = MulticlassClassificationEvaluator(predictionCol="prediction",labelCol="label")
    print("F1: %g" % (evaluator.evaluate(predictions)))
    pipelineFit.save("sentiment.model")

# Retrieve SparkSession instance
def getSparkSessionInstance(sparkConf):
  if ("sparkSessionSingletonInstance" not in globals()):
    globals()["sparkSessionSingletonInstance"] = SparkSession \
      .builder \
      .config(conf=sparkConf) \
      .getOrCreate()
  return globals()["sparkSessionSingletonInstance"]


def store_elasticache(time, rdd):
  try:
    # Get the singleton instance of SparkSession
    spark = getSparkSessionInstance(rdd.context.getConf())

    # Convert RDD[String] to RDD[Tweet] to DataFrame
    rowRdd = rdd.map(lambda w: Tweet(w))
    tweetsDataFrame = spark.createDataFrame(rowRdd)
    trainedModel = PipelineModel.load('sentiment.model')
    
    testDF = trainedModel.transform(tweetsDataFrame)
    # print("Before :-")
    # testDF.show(5)
    testDF.createOrReplaceTempView("tweets")
    # print("After :-", testDF.show(5))
    '''
    Train the model again and save it 
    '''
    # train_model(testDF)
    trainedModel.fit(testDF)
    print("Workinggggggggggggggg")
    trainedModel.save("sentiment.model")
    
    sentimentCount = spark.sql("select predictedSentiment, count(predictedSentiment) from tweets group by predictedSentiment")
    sentimentCount.show()
    r = redis.StrictRedis(host='localhost', port=6379, db=0, charset="utf-8", decode_responses=True)
    existing_data = r.hgetall(key)
    
    df_json = sentimentCount.toJSON()
    for row in df_json.collect():
      row = json.loads(row)
      sentiment = row['predictedSentiment']
      temp_count = int(row['count(predictedSentiment)'])
      if sentiment in existing_data:
        temp_count += int(existing_data[sentiment])
      r.hset(key, sentiment, temp_count)
      print(r.hgetall(key))
    '''
    Here we need to train the model with the rdd that we have
      1. Call the train model
      2. now retrieve the train model
    '''  
  except Exception as e:
    print(e)
    pass



# ===================================================
# = Initialize Spark and Streaming  Context session =
# ===================================================
sc = SparkContext("local[2]", "Tweet Streaming App")
sqlContext = SQLContext(sc)
ssc = StreamingContext(sc, 10)
ssc.checkpoint( "./tweets/")
# ssc.checkpoint( "file:/home/ubuntu/tweets/checkpoint/")

# ==========================================
# = Train the model if not already trained =
# ==========================================
is_model_trained = os.path.isdir("sentiment.model")
if not is_model_trained:
  train_model()


# socket_stream = ssc.socketTextStream("13.59.99.242", 5555)
socket_stream = ssc.socketTextStream("127.0.0.1", 5555)

tweetsDStream = socket_stream.window(20)
Tweet = namedtuple('Tweet', ("content"))

tweetsDStream.foreachRDD(store_elasticache)
ssc.start()
ssc.awaitTerminationOrTimeout(300)
