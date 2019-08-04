from A_star import PointSet, AStar, Node
from pysql import edges_points_from_all, line_info, line_detail_info
from calculation_method import euclid, subway_price

# label_1 = "<span ><span style='color: rgb(146, 151, 147);font-size: 9px;'>"
# label_2 = "</span>"


def a_star(start, end, weight_type):
    subway_gain, bus_gain, walk_loss, transfer_loss = 1, 1, 1, 1
    if weight_type == 0:
        subway_gain *= 120
        bus_gain *= 2
        walk_loss *= 120
    elif weight_type == 1:
        walk_loss *= 2
        transfer_loss *= 3
    elif weight_type == 2:
        subway_gain *= 120
        transfer_loss *= 100
        walk_loss *= 120

    network, points, mapping_id_to_name = edges_points_from_all()
    points[-1] = start
    points[-2] = end
    mapping_id_to_name[-1] = '起点'
    mapping_id_to_name[-2] = '终点'
    start_node = Node(-1, start)
    end_node = Node(-2, end)
    # mapping_name_to_id = {value: key for key, value in mapping_id_to_name.items()}

    points_set = PointSet(points)
    # start_node = points_set.points[mapping_name_to_id[start]]
    # end_node = points_set.points[mapping_name_to_id[end]]

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


def route_planning_personal(route, start, end):
    if route is None:
        return None
    # route is a list of id
    subway_rate = 12 * 60
    bus_rate = 6 * 60
    walk_rate = 1 * 60
    edges, points, mapping_id_to_name = edges_points_from_all()
    # start = [113.28612975176703, 22.985081433339884]
    # end = [113.43205579876216, 23.044557805553685]
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
    json_info['z_route'] = position
    print(json_info)
    return json_info, compare


def route_detailed_info(digital_route, start, end):
    edges, points, mapping_id_to_name = edges_points_from_all()
    points[-1] = start
    points[-2] = end
    mapping_id_to_name[-1] = '起点'
    mapping_id_to_name[-2] = '终点'
    # digital_route = [-1, 7035, 1389, 1388, 6990, 6973, 6975, 6976, 6977, 81, 82, -2]
    detail = dict()
    detail['set_up_site'] = '广东工业大学'
    detail['destination_site'] = '华南理工大学'
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
    detail['subway_block'] = station_info
    return detail


if __name__ == '__main__':
    start, end, weight_type = [113.23614743624573, 23.313819286105772], [113.36626876192827, 23.314073128074728], 0
    route = a_star(start, end, weight_type)
    print(route)
"""

            if len(lines_distributed[key]) > 2:
                section['h_middle'] = dict()
                for i in range(1, len(lines_distributed[key]) - 1):
                    section['h_middle']['cross' + str(i)] = mapping_id_to_name[lines_distributed[key][i]]
                    
                if weight_type == 0:
        json_info['convenient_type'] = '用时少'
    elif weight_type == 1:
        json_info['convenient_type'] = '花费少'
    elif weight_type == 2:
        json_info['convenient_type'] = '舒适高'
"""
