import os
import json
from  datetime import datetime

import numpy as np
from tables import open_file
from tables.nodes import filenode


class Storage(object):
    '''
    Storage is a wrapper class around PyTable file. It helps dealing with
    node and table creation and provide support for JSON node.
    '''

    def __init__(self, path):
        mode = 'a' if os.path.exists(path) else 'w'
        self._db = open_file(path, mode=mode)

    def get_node(self, path):
        '''
        Return the node at the given path. If the node doesn't exist then
        None is returned.

        Arguments:
            str path: path of the node

        Return:
            None or PyTables Node() instance
        '''
        path = _norm_node_path(path)
        return self._db.get_node(path) if path in self._db else None

    def remove_path(self, path):
        '''
        Return the node at the given path. It does do anything if the node
        does exist.

        Arguments:
            str path: path of the node

        Returns:
            None
        '''
        path = _norm_node_path(path)
        if path in self._db:
            self._db.remove_node(path, recursive=True)

    def create_dir(self, path):
        '''
        Create a directory node at the given path.

        Arguments:
            str name: name of the directory
            bool force: if force is True then the directory is removed first

        Return:
            None
        '''
        group = '/'
        for name in path[1:].split('/'):
            p = group + name
            if p not in self._db:
                self._db.create_group(group, name)
            group = p + '/'

    def update_table(self, path, serie, cols):
        if len(serie) == 0:
            return

        path = _norm_node_path(path)
        dir_path, basename = os.path.split(path)
        self.create_dir(dir_path)

        tbl = self.get_node(path)
        if tbl is None:
            tbl = self._db.create_table(dir_path, basename, cols)

        print 'Updating table %s (%d values)' % (path, len(serie))
        data = np.rec.array(serie)
        tbl.append(data)
        tbl.flush()
        tbl.close()

    def put_json(self, path, content):
        '''
        JSON encode the given and put it at the given path.

        Arguments:
            str path: where to store the content
            obj content: content to be stored as JSON

        Return:
            None
        '''
        path = _norm_node_path(path)
        _, dir_name, node_name = path.split('/')
        dir_path = '/' + dir_name
        if dir_path not in self._db:
            self._db.create_group("/", dir_name)
        else:
            self._db.remove_node(path)

        fnode = filenode.new_node(self._db, where=dir_path, name=node_name)
        fnode.attrs.content_type = 'text/plain; charset=us-ascii'
        fnode.write(json.dumps(content))
        fnode.flush()
        fnode.close()
        self._db.flush()

    def get_json(self, path):
        '''
        JSON decode the content at the given path and return it.

        Arguments:
            str path: where to store the content

        Return:
            JSON unmarshalled content
        '''

        node = self.get_node(path)
        if node is None:
            return None

        with filenode.open_node(node, 'r') as f:
            content = json.loads(f.read())

        node.close()
        return content

    def close(self):
        '''
        Close the data store.
        '''
        self._db.flush()
        self._db.close()


def parse_date(d):
    try:
        return datetime.strptime(d, '%Y-%m-%d')
    except ValueError:
        return None


def _norm_node_path(node_path):
    return node_path.replace('^', 'INDEX_')
