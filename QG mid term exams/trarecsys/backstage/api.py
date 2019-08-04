from flask import Flask, jsonify, make_response, request, url_for
from flask_cors import CORS
from interface import a_star, route_planning_personal, route_detailed_info, a_star_long_term, planning
from pysql import blocks_info
from calculation_method import euclid
import numpy as np

app = Flask(__name__)
CORS(app, resources=r'/*')
# deal with the cross-domain problem
departure = list()
destination = list()
routes = list()

subway_edges, bus_edges, subway_stations, stations, mapping_id_to_name = blocks_info()
edges = subway_edges + bus_edges

@app.route('/todo/api/v1.5/route/<int:id>', methods=['GET'])
def get_task(id):
    global routes, departure, destination, edges, stations, mapping_id_to_name
    info = route_detailed_info(routes[id], departure, destination, edges, stations, mapping_id_to_name)

    return jsonify(info)


@app.route('/todo/api/v1.5/route', methods=['POST'])
def get_route():
    """
    get the post which should be the information of the departure and destination
    :return: a json form data which contain the route information
    """
    global routes, departure, destination
    type_route = request.form.get('type', type=int, default=None)
    departure_x = request.form.get('departure_x', type=float, default=None)
    departure_y = request.form.get('departure_y', type=float, default=None)
    destination_x = request.form.get('destination_x', type=float, default=None)
    destination_y = request.form.get('destination_y', type=float, default=None)
    departure = [departure_x, departure_y]
    destination = [destination_x, destination_y]
    print(departure, destination)
    sorted_infos, routes = planning(departure, destination, type_route)
    print(sorted_infos)
    if not len(sorted_infos):
        return jsonify({'status': '0'})
    return jsonify(sorted_infos)


@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)