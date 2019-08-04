import pymysql

def edges_points_from_db(route_type='bus'):
    db = pymysql.connect('localhost', 'rrot', 'hzt123', route_type)
    cursor = db.cursor()
    cursor.execute('select * from ' + route_type + '.' + route_type + '_edges')
    edges_data = cursor.fetchall()
    edges = list()
    for row in edges_data:
        if row[4] is None or row[5] is None:
            continue
        edges.append([row[4], row[5]])

    cursor.execute('select * from ' + route_type + '.' + 'stations')
    stations_data = cursor.fetchall()
    mapping = dict()
    points = dict()
    for row in stations_data:
        mapping[row[0]] = row[1]
        point = [row[2], row[3]]
        points[row[0]] = point
    db.close()
    return edges, points, mapping
def edges_points_subway(route_type='subway'):
    db = pymysql.connect('localhost', 'root', 'hzt123', route_type)
    cursor = db.cursor()
    cursor.execute('select * from ' + route_type + '.' + route_type + '_edges')
    edges_data = cursor.fetchall()
    edges = list()
    for row in edges_data:
        if row[4] is None or row[5] is None:
            continue
        edges.append([row[4], row[5]])

    cursor.execute('select * from ' + route_type + '.' + route_type + '_stations')
    stations_data = cursor.fetchall()
    mapping = dict()
    points = dict()
    for row in stations_data:
        mapping[row[0]] = row[1]
        point = [row[3], row[4]]
        points[row[0]] = point
    db.close()
    return edges, points, mapping
def edges_points_from_all():
    db = pymysql.connect('localhost', 'root', 'hzt123', 'trases')
    cursor = db.cursor()
    cursor.execute('select * from ' + 'trases' + '.' + 'edges_')
    edges_data = cursor.fetchall()
    edges = list()
    for row in edges_data:
        if row[4] is None:
            edges.append([row[1], row[2], row[3], -1])
        else:
            edges.append([row[1], row[2], row[3], row[4]])
    cursor.execute('select * from ' + 'trases' + '.' + 'stations')
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
    db = pymysql.connect('localhost', 'root', 'hzt123', 'trases')
    cursor = db.cursor()
    cursor.execute('select * from ' + 'trases' + '.' + 'subway_line')
    subway_line_data = cursor.fetchall()
    subway_line_mapping_id_to_name = dict()
    for row in subway_line_data:
        subway_line_mapping_id_to_name[row[0]] = row[5]
    cursor.execute('select * from ' + 'trases' + '.' + 'bus_line')
    bus_line_data = cursor.fetchall()
    bus_line_mapping_id_to_name = dict()
    for row in bus_line_data:
        bus_line_mapping_id_to_name[row[0]] = row[5]
    db.close()

    return subway_line_mapping_id_to_name, bus_line_mapping_id_to_name
def line_detail_info():
    db = pymysql.connect('localhost', 'root', 'hzt123', 'trases')
    cursor = db.cursor()
    cursor.execute('select * from ' + 'trases' + '.' + 'subway_line')
    subway_line_data = cursor.fetchall()
    subway_line = list(map(list, subway_line_data))
    for line in subway_line:
        line[4] = str(line[4])
    cursor.execute('select * from ' + 'trases' + '.' + 'bus_line')
    bus_line_data = cursor.fetchall()
    bus_line = list(map(list, bus_line_data))
    for line in bus_line:
        line[4] = str(line[4])
    db.close()

    return subway_line, bus_line
def get_stations():
    db = pymysql.connect('localhost', 'root', 'hzt123', 'trases')
    cursor = db.cursor()
    cursor.execute('select * from ' + 'trases' + '.' + 'stations')
    stations_data = cursor.fetchall()
    mapping = dict()
    for row in stations_data:
        mapping[row[0]] = row[1]
    db.close()
    return mapping

def get_location(geohash):
    db = pymysql.connect('localhost', 'root', 'hzt123', 'trases')
    cursor = db.cursor()
    cursor.execute('select id, lon, lat from stations where five_geohash = "{}" '.format(geohash[:5]))
    station_location = cursor.fetchall()
    station_location = list(map(list, station_location))
    return station_location

def load_root(rootid, start, end):
    db = pymysql.connect('localhost', 'route', 'hzt123', 'trases')
    cursor = db.cursor()
    cursor.execute('INSERT INTO  route (route_0, route_1, route_2, start_code, end_code) VALUES ("{}", "{}", "{}", "{}", "{}")'.format(str(rootid[0]), str(rootid[1]), str(rootid[2]), start, end))
    station_location = cursor.fetchall()
    db.commit()

