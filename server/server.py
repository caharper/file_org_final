from flask import request, jsonify, Flask
import json
from flask_cors import CORS
from persistence import Persistence


persistence = Persistence()

app = Flask(__name__)

CORS(app)

@app.route('/')
def hello_world():
    return 'Hello, World!'

@app.route('/get_by_name', methods=["POST"])
def get_by_name():
    data = json.loads(request.data)
    first_name = data['first_name']
    last_name = data['last_name']
    return persistence.get_by_name(first_name, last_name)

@app.route('/get_by_city', methods=["POST"])
def get_by_city():
    data = json.loads(request.data)
    city = data['city']
    return persistence.get_by_city(city)

@app.route('/get_by_route', methods=["POST"])
def get_by_route():
    data = json.loads(request.data)
    route = data['route']
    return persistence.get_by_city(route)

@app.route('/get_is_there_a_route', methods=["POST"])
def get_is_there_a_route():
    data = json.loads(request.data)
    route_a = data['route_a']
    route_b = data['route_b']
    return persistence.get_by_name(route_a, route_b)


if __name__ == '__main__': 
    app.run(host='0.0.0.0', debug=True)