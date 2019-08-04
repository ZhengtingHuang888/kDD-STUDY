from a_star_for_short_term import PointSet, AStar, Node
from a_star_for_long_term import AStarLong
from pysql import edges_points_from_all, line_info, line_detail_info, blocks_info
from calculation_method import euclid, subway_price
import numpy as np


def planning(departure, destination, type_route):
    routes = list()
    routes_num = 1
    if euclid(departure, destination) < 1e4:
        routes_num *= 2
        for i in range(routes_num):
            digital_route = a_star(departure, destination, i, type_route)
            routes.append(digital_route)
    else:
        for i in range(routes_num):
            digital_route = a_star_long_term(departure, destination, i, type_route)
            routes.append(digital_route)
    print(routes)
    information = list()
    comparisons = list()
    for i in range(routes_num):
        info, campare = route_planning_personal(routes[i], departure, destination)
        information.append(info)
        comparisons.append(campare)
    price_min = np.inf
    price_min_line = 0
    for i in range(routes_num):
        if comparisons[i][0] < price_min:
            price_min = comparisons[i][0]
            price_min_line = i
    information[price_min_line]['convenient_type'].append('花费少')
    time_min = np.inf
    time_min_line = 0
    for i in range(routes_num):
        if comparisons[i][1] < time_min:
            time_min = comparisons[i][1]
            time_min_line = i
    information[time_min_line]['convenient_type'].append('用时少')
    transfer_min = np.inf
    transfer_min_line = 0
    for i in range(routes_num):
        if comparisons[i][2] < transfer_min:
            transfer_min = comparisons[i][2]
            transfer_min_line = i
    information[transfer_min_line]['convenient_type'].append('换乘少')

    sorted_info = sorted(information, key=lambda e: len(e.__getitem__('convenient_type')), reverse=True)

    return sorted_info, routes


def weights(weight_type, type_route):
    subway_gain, bus_gain, walk_loss, transfer_loss = 1, 1, 5, 1
    if weight_type == 0:
        subway_gain *= 120
        transfer_loss *= 100
    elif weight_type == 1:
        bus_gain *= 1.5
        walk_loss *= 100

    if type_route == 2:
        transfer_loss *= 3
    else:
        subway_gain *= 2

    return subway_gain, bus_gain, walk_loss, transfer_loss


def a_star(start, end, weight_type, type_route):
    subway_gain, bus_gain, walk_loss, transfer_loss = weights(weight_type, type_route)

    network, points, mapping_id_to_name = edges_points_from_all()
    points[-1] = start
    points[-2] = end
    mapping_id_to_name[-1] = '起点'
    mapping_id_to_name[-2] = '终点'
    start_node = Node(-1, start)
    end_node = Node(-2, end)

    points_set = PointSet(points)

    a_star_class = AStar(network, points_set.points, start_node, end_node)
    a_star_class.reset_weights(subway_gain, bus_gain, walk_loss, transfer_loss)
    if a_star_class.start():
        result = list()
        for i in a_star_class.path_list:
            result.append(mapping_id_to_name[i])
        print(result[::-1])
        return a_star_class.path_list[::-1]

    else:
        print('False to a star')
        return None


def a_star_long_term(start, end, weight_type, type_route):
    subway_gain, bus_gain, walk_loss, transfer_loss = weights(weight_type, type_route)

    subway_edges, bus_edges, subway_stations, stations, mapping_id_to_name = blocks_info()

    stations[-1] = start
    stations[-2] = end
    mapping_id_to_name[-1] = '起点'
    mapping_id_to_name[-2] = '终点'
    points_set = PointSet(stations)
    subway_set = PointSet(subway_stations)
    start_node = Node(-1, start)
    end_node = Node(-2, end)

    start_node_subway = 0
    end_node_subway = 0

    min_dist = np.inf
    for node in [value for key, value in subway_set.points.items()]:
        dist = start_node.ecl(node)
        if dist < min_dist:
            min_dist = dist
            start_node_subway = node

    min_dist = np.inf
    for node in [value for key, value in subway_set.points.items()]:
        dist = end_node.ecl(node)
        if dist < min_dist:
            min_dist = dist
            end_node_subway = node

    if start_node_subway.id == end_node_subway.id:
        subway_route = [start_node_subway.id, end_node_subway.id]
    else:
        subway_a_star = AStarLong(subway_edges, points_set.points, start_node_subway, end_node_subway)
        subway_a_star.reset_transfer_loss(transfer_loss)
        if not subway_a_star.start():
            print('error in subway planning')
            return None
        subway_route = subway_a_star.path_list[::-1]

    front_a_star = AStar(bus_edges, points_set.points, start_node, start_node_subway)
    front_a_star.reset_weights(subway_gain, bus_gain, walk_loss, transfer_loss)
    if not front_a_star.start():
        print('error in front planning')
        return None
    front_part = front_a_star.path_list[::-1]

    back_a_star = AStar(bus_edges, points_set.points, end_node_subway, end_node)
    back_a_star.reset_weights(subway_gain, bus_gain, walk_loss, transfer_loss)
    if not back_a_star.start():
        print('error in back planning')
        return None
    back_part = back_a_star.path_list[::-1]

    return front_part + subway_route[1:-1] + back_part


def route_planning_personal(route, start, end):
    if route is None:
        return None
    # route is a list of id
    subway_rate = 12 * 60
    bus_rate = 6 * 60
    walk_rate = 1 * 60
    edges, points, mapping_id_to_name = edges_points_from_all()
    points[-1] = start
    points[-2] = end
    mapping_id_to_name[-1] = '起点'
    mapping_id_to_name[-2] = '终点'
    subway_line_mapping, bus_line_mapping = line_info()
    subway_distance = 0
    bus_distance = 0
    walk_distance = 0
    lines = list()
    for i in range(len(route) - 1):
        section = euclid(points[route[i]], points[route[i + 1]])
        is_walk = True
        for edge in edges:
            if edge[0] == route[i] and edge[1] == route[i + 1]:
                if edge[3] <= 31:
                    subway_distance += section
                else:
                    bus_distance += section
                is_walk = False
                if edge[3] not in lines:
                    lines.append(edge[3])
        if is_walk:  # walk
            walk_distance += section
    num_bus_lines = 0
    for line in lines:
        if line > 31:
            num_bus_lines += 1
    print('******', lines, '******')
    price = subway_price(subway_distance) + num_bus_lines * 2
    set_up_site = mapping_id_to_name[route[1]]
    time = int(subway_distance / subway_rate + bus_distance / bus_rate + walk_distance / walk_rate)
    lines_ch = list()
    for line in lines:
        if line in subway_line_mapping:
            section = subway_line_mapping[line]
            if section not in lines_ch:
                lines_ch.append(section)
        if line in bus_line_mapping:
            section = '公交'
            if section not in lines_ch:
                lines_ch.append(section)
    position = list()
    for i in range(len(route)):
        road = dict()
        start = points[route[i]]
        road['x'], road['y'] = str(start[0]), str(start[1])
        position.append(road)
    json_info = dict()
    json_info['time'] = str(time) + '分钟'
    json_info['mile'] = str(int(subway_distance + bus_distance + walk_distance)) + '米'
    json_info['walk'] = str(int(walk_distance)) + '米'
    json_info['price'] = str(price) + '元'
    json_info['transfer'] = str(len(lines)) + '站'
    json_info['convenient_type'] = list()
    compare = [price, time, len(lines)]

    json_info['set_up_site'] = set_up_site + '上车'
    len_lines = len(lines_ch)
    if len_lines > 5:
        len_lines = 5
    for i in range(len_lines):
        json_info['line_' + str(i)] = lines_ch[i]
    if len_lines < 5:
        for i in range(len_lines, 5):
            json_info['line_' + str(i)] = 'null'
    if json_info['line_0'] == 'null':
        json_info['line_0'] = '步行'
    json_info['z_route'] = position
    print(json_info)
    return json_info, compare


def route_detailed_info(digital_route, start, end, edges, points, mapping_id_to_name):
    points[-1] = start
    points[-2] = end
    mapping_id_to_name[-1] = '起点'
    mapping_id_to_name[-2] = '终点'
    station_info = list()
    lines, bus_lines = line_detail_info()
    lines.extend(bus_lines)
    lines_distributed = dict()
    for i in range(len(digital_route) - 1):
        for edge in edges:
            if edge[0] == digital_route[i] and edge[1] == digital_route[i + 1] and edges[3]:
                if edge[3] not in list(lines_distributed.keys()):
                    lines_distributed[edge[3]] = list()
                if digital_route[i] not in lines_distributed[edge[3]]:
                    lines_distributed[edge[3]].append(digital_route[i])
                if digital_route[i + 1] not in lines_distributed[edge[3]]:
                    lines_distributed[edge[3]].append(digital_route[i + 1])
    print(lines_distributed)
    for key in list(lines_distributed.keys()):
        section = dict()
        if key == list(lines_distributed.keys())[0]:
            walk_distance = euclid(points[digital_route[0]], points[digital_route[1]])
            section['a_changes'] = '步行%.2f米至%s' % \
                                   (walk_distance, mapping_id_to_name[lines_distributed[key][0]])
            section['b_line'] = 'null'
            section['e_time'] = 'null'
            for line in lines:
                if line[0] == key:
                    section['b_line'] = line[-1]
                    section['e_time'] = '末班车: ' + line[4] + ' ' + line[3]
            section['c_start'] = mapping_id_to_name[lines_distributed[key][0]]
            section['d_end'] = mapping_id_to_name[lines_distributed[key][-1]]
            section['f_number_stations'] = str(len(lines_distributed[key])) + '站'
            section['g_color'] = str(key)
            section['h_middle'] = 'null'
            if len(lines_distributed[key]) > 2:
                section['h_middle'] = dict()
                for i in range(1, len(lines_distributed[key]) - 1):
                    section['h_middle']['cross_' + str(i)] = mapping_id_to_name[lines_distributed[key][i]]
            station_info.append(section)

        else:
            section['a_changes'] = '转乘' + mapping_id_to_name[lines_distributed[key][0]]
            section['b_line'] = 'null'
            section['e_time'] = 'null'
            for line in lines:
                if line[0] == key:
                    section['b_line'] = line[-1]
                    section['e_time'] = '末班车: ' + line[4] + ' ' + line[3]
            section['c_start'] = mapping_id_to_name[lines_distributed[key][0]]
            section['d_end'] = mapping_id_to_name[lines_distributed[key][-1]]
            section['f_number_stations'] = str(len(lines_distributed[key])) + '站'
            section['g_color'] = str(key)
            section['h_middle'] = 'null'
            if len(lines_distributed[key]) > 2:
                section['h_middle'] = dict()
                for i in range(1, len(lines_distributed[key]) - 1):
                    section['h_middle']['cross_' + str(i)] = mapping_id_to_name[lines_distributed[key][i]]
            station_info.append(section)
    return station_info
