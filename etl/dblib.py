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
        return self._db.get_node(_norm_node_path(path))

    def create_dir(self, name, force=False):
        path = '/' + name
        if path in self._db:
            if force:
                self._db.remove_node(path, recursive=True)
                self._db.create_group("/", name)
        else:
            self._db.create_group("/", name)

    def update_table(self, group, table, serie, cols):
        print 'Updating table /%s/%s (%d values)' % (group, table, len(serie))
        if len(serie) == 0:
            return

        tbl = self.get_table('/{0}/{1}'.format(group, table))
        if tbl is None:
            tbl = self._db.create_table('/' + group, _norm_node_path(table), cols)

        data = np.rec.array(serie)
        tbl.append(data)
        tbl.flush()
        tbl.close()

    def create_json_node(self, dir_name, node_name, content):
        dir_path = _norm_node_path('/{0}'.format(dir_name))
        node_path = _norm_node_path('/{0}/{1}'.format(dir_name, node_name))
        if dir_path not in self._db:
            self._db.create_group("/", dir_name)
        else:
            self._db.remove_node(node_path)

        fnode = filenode.new_node(self._db, where=dir_path, name=_norm_node_path(node_name))
        fnode.attrs.content_type = 'text/plain; charset=us-ascii'
        fnode.write(json.dumps(content))
        fnode.close()
        self._db.flush()

    def read_json_node(self, *parts):
        path = '/'.join(parts)
        path = _norm_node_path('/' + path)
        if path not in self._db:
            return None

        node = self._db.get_node(path)
        with filenode.open_node(node, 'r') as f:
            content = json.loads(f.read())

        node.close()
        return content

    def get_table(self, node_path):
        path = _norm_node_path(node_path)
        return self._db.get_node(path) if path in self._db else None

    def close(self):
        self._db.close()


def parse_date(d):
    try:
        return datetime.strptime(d, '%Y-%m-%d')
    except ValueError:
        return None


def _norm_node_path(node_path):
    return node_path.replace('^', 'INDEX_')
