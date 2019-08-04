import pymysql
from pygeohash import encode
from geopy.distance import vincenty
import numpy as np
from bus.conversion_changee import *

planning_lists = [[113.22422, 23.27072], [113.39922, 22.87072], [113.53422, 23.40072], [113.83922, 23.710720000000002], [113.48922, 23.00572], [113.44422, 23.00572], [113.31422, 23.09072], [113.31422, 23.05072], [113.70922, 23.530720000000002], [113.26921999999999, 23.22572], [113.39922, 23.62072], [113.79422, 23.530720000000002], [113.35422, 23.00572], [113.35422, 23.05072], [113.57422, 22.74072], [113.44422, 23.09072], [113.35422, 23.62072], [113.66422, 23.44572], [113.97422, 23.92072], [113.22422, 23.18072], [113.92922, 23.88572], [113.66422, 22.65572], [113.44422, 23.57572], [113.79422, 23.57572], [113.70922, 23.75072], [113.17922, 23.355719999999998], [113.70922, 23.62072], [113.79422, 23.84072], [113.09422, 23.40072], [113.44422, 23.18072], [113.44422, 23.530720000000002], [113.26921999999999, 23.13572], [113.83922, 23.57572], [113.48922, 23.05072], [113.48922, 23.62072], [113.75422, 23.31072], [113.88422, 23.75072], [113.61922, 22.70072], [113.53422, 22.91572], [113.48922, 22.87072], [113.17922, 23.40072], [113.35422, 23.40072], [113.57422, 23.18072], [113.88422, 23.40072], [113.66422, 23.57572], [113.88422, 23.92072], [113.57422, 23.40072], [113.35422, 23.22572], [113.70922, 23.22572], [113.39922, 23.355719999999998], [113.53422, 23.31072], [113.35422, 23.18072], [113.31422, 23.355719999999998], [113.39922, 23.66572], [113.31422, 23.18072], [113.35422, 23.57572], [113.66422, 23.66572], [113.44422, 23.27072], [113.88422, 23.88572], [113.31422, 23.40072], [114.01422, 23.79572], [113.97422, 23.79572], [113.79422, 23.22572], [113.57422, 23.62072], [113.44422, 22.87072], [113.66422, 23.22572], [113.35422, 22.87072], [113.48922, 23.09072], [113.48922, 23.27072], [113.57422, 23.31072], [113.97422, 23.84072], [113.44422, 22.83072], [113.31422, 23.44572], [113.53422, 23.355719999999998], [113.35422, 23.13572], [113.44422, 23.355719999999998], [113.31422, 23.27072], [113.75422, 23.57572], [113.53422, 22.83072], [113.39922, 23.40072], [113.79422, 23.31072], [113.70922, 23.66572], [113.31422, 22.91572], [113.79422, 23.710720000000002], [113.48922, 22.960720000000002], [113.26921999999999, 23.18072], [113.35422, 23.530720000000002], [113.31422, 23.13572], [113.92922, 23.75072], [113.66422, 23.27072], [113.22422, 23.13572], [113.83922, 23.22572], [113.57422, 23.27072], [113.88422, 23.84072], [113.39922, 23.22572], [113.44422, 22.91572], [113.75422, 23.355719999999998], [113.35422, 23.49072], [113.75422, 23.18072]]

center = [113.39922, 23.05072]


def euclid(dot_a, dot_b):
    dist = vincenty(dot_a[::-1], dot_b[::-1]).meters
    return dist


def manhattan(dot_a, dot_b):
    return abs(dot_a[0] - dot_b[0]) + abs(dot_a[1] - dot_b[1])


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


def add_to_database(routes, start, end):
    db = pymysql.connect('localhost', 'root', 'hzt123', 'trases')
    cursor = db.cursor()
    cursor.execute('INSERT INTO  route (route_0, route_1, route_2, start_code, end_code, start_fivecode, end_fivecode'
                   ') VALUES ("{}", "{}", "{}", '
                   '"{}", "{}", "{}", "{}")'.format(str(routes[0]), str(routes[1]), str(routes[2]), start, end, start[:5], end[:5]))
    db.commit()
    db.close()

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


class Node:
    """
    the node in a* algorithms shows the position and the father node
    """
    def __init__(self, node_id, position):
        """
        initialize the node-id and the position
        :param node_id: the number to mark the node
        :param position: a 1 by 2 list which contain the position info
        of node. like '[lat, lng]'
        """
        self.id = node_id
        self.position = position  # like [1, 2]
        self.father = None
        self.g = 0
        self.h = 0

    def mht(self, node):
        # cal the manhattan distance between two nodes
        return manhattan(self.position, node.position)

    def ecl(self, node):
        # cal the euclid distance between two nodes
        return euclid(self.position, node.position)

    def set_g(self, g):
        # self-explanatory
        self.g = g

    def set_h(self, h):
        # self-explanatory
        self.h = h

    def set_father(self, node):
        # set the father node of the node
        self.father = node


class AStar:
    """
    the main algorithms of a*
    """
    def __init__(self, network, node_set, start_node, end_node):
        """
        initialize the info of a*
        :param network: the edges of graph
        :param node_set: a dict between node_id and node
        :param start_node: self-explanatory
        :param end_node: self-explanatory
        """
        self.open_list = list()
        self.close_list = list()
        # init the two list
        self.network = network
        self.node_set = node_set
        self.start_node = start_node
        self.end_node = end_node

        self.current_node = start_node
        self.current_line = None
        self.path_list = list()
        self.subway_gain = 12
        self.bus_gain = 6
        self.walk_loss = 100
        self.transfer_loss = 5

    def reset_weights(self, subway_gain, bus_gain, walk_loss, transfer_loss):
        self.subway_gain = subway_gain
        self.bus_gain = bus_gain
        self.walk_loss = walk_loss
        self.transfer_loss = transfer_loss

    def get_min_f_node(self):
        # find the min f in open list (f = g + h, using manhattan distance)
        min_node = self.open_list[0]
        # print('-open_list-')
        # for node in self.open_list:
        #    print(node.id, end=' ')
        # print('\n---')
        # print('current node id: ', self.current_node.id)
        # print('node loss:')
        min_f = np.inf
        for node in self.open_list:
            next_line = self.get_line(node)
            node_f = node.g + node.h
            if node.id <= 224:
                node_f /= self.subway_gain
            if node.id > 224:
                node_f /= self.bus_gain
            # node_f = self.f_w ith_type(node_f, self.get_type(node))
            if next_line is None:
                node_f *= self.walk_loss
            elif next_line != self.current_line:
                # and next_line is not None -- this is error usage
                # if next line is none, it means the node has no line with current node
                # but if the current switched, it maybe have line. so next line can be none

                # if the next line is different from current line
                # the loss must be bigger
                node_f *= self.transfer_loss
            # print('node id :%d node f :%f next line: %s' % (node.id, node_f, str(next_line)))
            if node_f < min_f:
                min_node = node
                min_f = node_f
        return min_node

    def get_type(self, node):
        # get the type of line of the current node between the node given
        for net in self.network:
            if net[0] == self.current_node.id and net[1] == node.id:
                return net[2]

    def get_line(self, node):
        # get the line of the current node between the node given
        for net in self.network:
            if net[0] == self.current_node.id and net[1] == node.id:
                return net[3]
        return None

    def node_in_open_list(self, node):
        # check if the node is in the open list
        for open_node in self.open_list:
            if open_node.position == node.position:
                return True
        return False

    def node_in_close_list(self, node):
        # check if the node is in the close list
        for close_node in self.close_list:
            if close_node.position == node.position:
                return True
        return False

    def get_node_from_open_list(self, node):
        # self-explanatory
        for open_node in self.open_list:
            if open_node.position == node.position:
                return open_node
        return None

    def search_one_node(self, node, d=1):
        # search one node with current node
        if self.node_in_close_list(node):
            # the node is already searched
            return

        if not self.node_in_open_list(node):
            # the node is unvisited
            if node.id is not None:
                node.set_g(node.mht(self.start_node))
                node.set_h(d * node.mht(self.end_node))
                node.set_father(self.current_node)
                self.open_list.append(node)

        elif node.g > self.current_node.mht(self.start_node) + self.current_node.g:
            # if crossing the current is better than not
            # then the node should across the current node (set the father of node with current node)
            node.set_father(self.current_node)

    def search_near(self, r=1e3):
        # search the nodes nearby the current node
        for net in self.network:
            if self.current_node.id == net[0]:
                self.search_one_node(self.node_set[net[1]])

        min_dis = np.inf
        is_enough = False
        while True:
            num = 0
            for node in [value for key, value in self.node_set.items()]:
                dist = self.current_node.ecl(node)
                if dist < r:
                    if not self.node_in_close_list(node) and not self.node_in_open_list(node):
                        num += 1
                if min_dis > dist:
                    min_dis = dist
                    # print(min_dis)
                if num >= 3:
                    is_enough = True
                    break
            if is_enough:
                break
            r += 1e3

        for node in [value for key, value in self.node_set.items()]:
            if self.current_node.ecl(node) < r:
                self.search_one_node(node)

    def clean_father(self):
        # set all node's fathers with none
        nodes = list(self.node_set.values())
        for node in nodes:
            node.set_father(None)

    def set_the_line(self, node):
        # update the current line
        if node == self.start_node:
            return
        self.current_line = self.get_line(node)

    def start(self, d=1):
        # algorithm starts
        self.start_node.set_h(d * self.start_node.mht(self.end_node))
        self.open_list.append(self.start_node)
        while True:
            # print('current line:', self.current_line)
            next_node = self.get_min_f_node()
            self.set_the_line(next_node)
            self.current_node = next_node
            self.close_list.append(self.current_node)
            self.open_list.remove(self.current_node)

            self.search_near()

            if self.node_in_open_list(self.end_node):
                node = self.get_node_from_open_list(self.end_node)
                while True:
                    self.path_list.append(node.id)
                    if node.father:
                        node = node.father
                    else:
                        # print(self.path_list)
                        self.clean_father()
                        return True

            elif len(self.open_list) == 0:
                return False


class PointSet:

    def __init__(self, point_set):
        self.points = dict()
        for i in list(point_set.keys()):
            self.points[i] = Node(i, point_set[i])


def points_encode(start, end):
    lat1, lon1 = start[1], start[0]
    lat2, lon2 = end[1], end[0]
    return encode(lat1, lon1), encode(lat2, lon2)


if __name__ == '__main__':
    planning_routes = list()
    center_lon, center_lat = wgs84tobd09(center[0], center[1])
    center = [center_lon, center_lat]
    for planning_list in planning_lists:
        planning_lon, planning_lat = wgs84tobd09(planning_list[0], planning_list[1])
        planning_list = [planning_lon, planning_lat]
        for i in range(3):
            route = a_star(center, planning_list, i)
            planning_routes.append(route)
        start_code, end_code = points_encode(center, planning_list[0])
    # add_to_database(planning_routes, start_code, end_code)
        add_to_database(planning_routes, start_code, end_code)
