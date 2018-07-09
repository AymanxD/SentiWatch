import tweepy
import socket
import requests
import time
import csv
import stat
import os
import json
import re

class TwitterStreamListener(tweepy.StreamListener):
  def __init__(self, sc):
    super(TwitterStreamListener, self).__init__()
    self.client_socket = sc

  def on_status(self, status):
    tweet = self.get_tweet(status)
    self.client_socket.send((tweet[2]+"\n").encode('utf-8'))
    return True

  def on_error(self, status_code):
    print("Status Code: %s" % str(status_code))
    if status_code == 403:
      print("Forbidden: The request is understood, but the access is not allowed. Limit may be reached.")
      return False

  def get_tweet(self,tweet):
    text = tweet.text
    if hasattr(tweet, 'extended_tweet'):
      text = tweet.extended_tweet['full_text']
    return [str(tweet.user.id),tweet.user.screen_name,self.clean_str(text)]

  def clean_str(self, string):
    string = re.sub(r"\n|\t", " ", string)
    return string

def get_twitter_api():
  # ===========================
  # = Twitter API Credentials =
  # ===========================
  consumer_key = "tBDbDBUN5F21vRRpzqRX2EgXU"
  consumer_secret = "eZnK6uPaSF9U8xg1GfR1XzioBNsJxzEGGGd52K2KlNNbjCrXxO"
  access_token = "896630360-AnutnT5yQgeG13pEDMo53kSnnBRXbftIqgoHI6cS"
  access_token_secret = "kHcj1n0FcDYnViyrdO3g1z5b6eWwB0ps7NHj8sHCEYzao"
  host = "127.0.0.1"
  port = 5555
  s = socket.socket()
  s.bind((host, port))
  print("Listening on port: %s" % str(port))
  s.listen(5)
  c, addr = s.accept()
  print("Received request from: " + str(addr))
  auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
  auth.secure = True
  auth.set_access_token(access_token, access_token_secret)
  api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, retry_count=10, retry_delay=5, retry_errors=5)
  return c, api


if __name__ == '__main__':
  c, api = get_twitter_api()
  streamListener = TwitterStreamListener(c)
  myStream = tweepy.Stream(auth=api.auth, listener=streamListener, tweet_mode='extended')
  myStream.filter(track=['movie','movies'], async=True)

