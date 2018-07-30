import json, random, os, sys
from flask import Flask, request, current_app
from flask_cors import CORS, cross_origin
import redis

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

@app.route('/start_streaming')
@cross_origin()
def start_streaming():
  user_id = request.args.get('user_id')
  query = request.args.get('query')
  os.system("python3 ETL.py " + user_id + " " + query + " &")
  # Here we will just call the function start the streaming process
  return json.dumps([])
  
@app.route('/get_results')
@cross_origin()
def get_results():
  # This endpoint will call the function and pass the query parameter received
  # and return the sentiment counts in response
  user_id = request.args.get('user_id')
  r = redis.StrictRedis(host='dwh-db.0gx2x1.ng.0001.use2.cache.amazonaws.com', port=6379, db=0, charset="utf-8", decode_responses=True)
  return json.dumps(r.hgetall(user_id))
  
if __name__ == '__main__':
  app.run()


