import json, random
from flask import Flask, request, current_app
from flask_cors import CORS, cross_origin

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

@app.route('/start_streaming')
@cross_origin()
def start_streaming():
  print("post request here")
  # Here we will just call the function start the streaming process
  return json.dumps([])
  
@app.route('/get_results')
@cross_origin()
def get_results():
  # This endpoint will call the function and pass the query parameter received
  # and return the sentiment counts in response
  response = []
  temp = {}
  temp['name'] = "Joy"
  temp['value'] = random.randint(1,101)
  response.append(temp)
  temp = {}
  temp['name'] = "Anger"
  temp['value'] = random.randint(1,101)
  response.append(temp)
  return json.dumps(response)
  
if __name__ == '__main__':
  app.run()


