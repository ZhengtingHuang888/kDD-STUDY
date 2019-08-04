import pymysql


def blocks_info():
    db = pymysql.connect('localhost', 'fkylin', 'rosehip@10', 'travel')
    cursor = db.cursor()
    cursor.execute('select * from ' + 'travel' + '.' + 'edges_')
    edges_data = cursor.fetchall()
    subway_edges = list()
    bus_edges = list()
    for row in edges_data:
        if row[3] == 0:
            subway_edges.append([row[1], row[2], row[3], row[4]])
        else:
            if row[4] is None:
                bus_edges.append([row[1], row[2], row[3], -1])
            else:
                bus_edges.append([row[1], row[2], row[3], row[4]])
    cursor.execute('select * from ' + 'travel' + '.' + 'stations')
    stations_data = cursor.fetchall()
    mapping_id_to_name = dict()
    subway_stations = dict()
    stations = dict()
    for row in stations_data:
        mapping_id_to_name[row[0]] = row[1]
        point = [row[2], row[3]]
        stations[row[0]] = point
        if row[4] == 0:
            subway_stations[row[0]] = point
    db.close()
    return subway_edges, bus_edges, subway_stations, stations, mapping_id_to_name


def edges_points_from_all():
    db = pymysql.connect('localhost', 'fkylin', 'rosehip@10', 'travel')
    cursor = db.cursor()
    cursor.execute('select * from ' + 'travel' + '.' + 'edges_')
    edges_data = cursor.fetchall()
    edges = list()
    for row in edges_data:
        if row[4] is None:
            edges.append([row[1], row[2], row[3], -1])
        else:
            edges.append([row[1], row[2], row[3], row[4]])

    cursor.execute('select * from ' + 'travel' + '.' + 'stations')
    stations_data = cursor.fetchall()
    mapping = dict()
    points = dict()
    for row in stations_data:
        mapping[row[0]] = row[1]
        point = [row[2], row[3]]
        points[row[0]] = point
    db.close()
    return edges, points, mapping


def line_info():
    db = pymysql.connect('localhost', 'fkylin', 'rosehip@10', 'travel')
    cursor = db.cursor()
    cursor.execute('select * from ' + 'travel' + '.' + 'subway_line')
    subway_line_data = cursor.fetchall()
    subway_line_mapping_id_to_name = dict()
    for row in subway_line_data:
        subway_line_mapping_id_to_name[row[0]] = row[5]
    cursor.execute('select * from ' + 'travel' + '.' + 'bus_line')
    bus_line_data = cursor.fetchall()
    bus_line_mapping_id_to_name = dict()
    for row in bus_line_data:
        bus_line_mapping_id_to_name[row[0]] = row[5]
    db.close()

    return subway_line_mapping_id_to_name, bus_line_mapping_id_to_name


def line_detail_info():
    db = pymysql.connect('localhost', 'fkylin', 'rosehip@10', 'travel')
    cursor = db.cursor()
    cursor.execute('select * from ' + 'travel' + '.' + 'subway_line')
    subway_line_data = cursor.fetchall()
    subway_line = list(map(list, subway_line_data))
    for line in subway_line:
        line[4] = str(line[4])
    cursor.execute('select * from ' + 'travel' + '.' + 'bus_line')
    bus_line_data = cursor.fetchall()
    bus_line = list(map(list, bus_line_data))
    for line in bus_line:
        line[4] = str(line[4])
    db.close()

    return subway_line, bus_line


def get_stations():
    db = pymysql.connect('localhost', 'fkylin', 'rosehip@10', 'travel')
    cursor = db.cursor()
    cursor.execute('select * from ' + 'travel' + '.' + 'stations')
    stations_data = cursor.fetchall()
    mapping = dict()
    for row in stations_data:
        mapping[row[0]] = row[1]
    db.close()
    return mapping


