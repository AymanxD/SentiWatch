import sys
import os

arguments = " ".join(sys.argv[1:])
print(arguments)

os.system("python3 tweepy_stream.py " + arguments + " &")
os.system("python3 spark_streamer.py")

