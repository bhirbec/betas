import os
import json
from  datetime import datetime

import numpy as np
from tables import open_file
from tables.nodes import filenode


class Storage(object):

    def __init__(self, path):
        mode = 'a' if os.path.exists(path) else 'w'
        self._db = open_file(path, mode=mode)

    def get_node(self, path):
        path = _norm_node_path(path)
        return self._db.get_node(path) if path in self._db else None

    def create_dir(self, name, force=False):
        path = '/' + name
        if path in self._db:
            if force:
                self._db.remove_node(path, recursive=True)
                self._db.create_group("/", name)
        else:
            self._db.create_group("/", name)

    def update_table(self, group, table, serie, cols):
        if len(serie) == 0:
            return

        table = _norm_node_path(table)
        tbl = self.get_node('/{0}/{1}'.format(group, table))
        if tbl is None:
            tbl = self._db.create_table('/' + group, table, cols)

        print 'Updating table /%s/%s (%d values)' % (group, table, len(serie))
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
        self._db.close()


def parse_date(d):
    try:
        return datetime.strptime(d, '%Y-%m-%d')
    except ValueError:
        return None


def _norm_node_path(node_path):
    return node_path.replace('^', 'INDEX_')
