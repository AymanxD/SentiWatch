import sys
import os

arguments = " ".join(sys.argv[2:])
user_id = sys.argv[1]

# Executes tweepy_stream to stream tweets with tweepy
os.system("python3 tweepy_stream.py " + arguments + " &")

# Executs spark_streamer to tranform tweets and gather their sentiments
os.system("python3 spark_streamer.py " + user_id)
