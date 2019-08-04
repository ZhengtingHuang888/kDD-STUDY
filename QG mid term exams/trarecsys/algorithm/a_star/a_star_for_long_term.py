import numpy as np


class AStarLong:
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

        self.transfer_loss = 3

    def reset_transfer_loss(self, transfer_loss):
        self.transfer_loss = transfer_loss

    def get_min_f_node(self):
        # find the min f in open list (f = g + h, using manhattan distance)
        min_node = self.open_list[0]
        print('-open_list-')
        for node in self.open_list:
            print(node.id, end=' ')
        print('\n---')
        print('current node id: ', self.current_node.id)
        print('node loss:')
        min_f = np.inf
        for node in self.open_list:
            next_line = self.get_line(node)
            node_f = node.g + node.h
            if next_line != self.current_line:
                node_f *= self.transfer_loss
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
        print('now search the points nearby the current point')
        for net in self.network:
            if self.current_node.id == net[0]:
                self.search_one_node(self.node_set[net[1]])

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
            print('current node:', self.current_node.id)
            next_node = self.get_min_f_node()
            self.set_the_line(next_node)
            self.current_node = next_node
            self.close_list.append(self.current_node)
            self.open_list.remove(self.current_node)
            self.search_near()

            if self.node_in_open_list(self.end_node):
                node = self.get_node_from_open_list(self.end_node)
                print('yes')
                while True:
                    self.path_list.append(node.id)
                    if node.father:
                        node = node.father
                    else:
                        # print(self.path_list)
                        self.clean_father()
                        return True

            elif len(self.open_list) == 0:
                print('now there has no point in open list')
                return False













