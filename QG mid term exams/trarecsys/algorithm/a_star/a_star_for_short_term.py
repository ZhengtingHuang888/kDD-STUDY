import numpy as np
from calculation_method import manhattan, euclid


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
        self.r = 2e3

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

    def search_near(self):
        # search the nodes nearby the current node
        for net in self.network:
            if self.current_node.id == net[0]:
                self.search_one_node(self.node_set[net[1]])

        for node in [value for key, value in self.node_set.items()]:
            if self.current_node.ecl(node) < self.r:
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
            print('current node: %d' % self.current_node.id)
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
















