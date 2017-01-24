import os
import json
from  datetime import datetime

import numpy as np
from tables import open_file
from tables.nodes import filenode


def open_db(path):
    mode = 'a' if os.path.exists(path) else 'w'
    return open_file(path, mode=mode)


def update_table(h5file, group, table, serie, cols):
    print 'Updating table /%s/%s (%d values)' % (group, table, len(serie))
    if len(serie) == 0:
        return

    tbl = get_table(h5file, '/{0}/{1}'.format(group, table))
    if tbl is None:
        tbl = h5file.create_table('/' + group, _norm_node_path(table), cols)

    data = np.rec.array(serie)
    tbl.append(data)
    tbl.flush()
    tbl.close()

def create_json_node(h5file, dir_name, node_name, content):
    dir_path = _norm_node_path('/{0}'.format(dir_name))
    node_path = _norm_node_path('/{0}/{1}'.format(dir_name, node_name))
    if dir_path not in h5file:
        h5file.create_group("/", dir_name)
    else:
        h5file.remove_node(node_path)

    fnode = filenode.new_node(h5file, where=dir_path, name=_norm_node_path(node_name))
    fnode.attrs.content_type = 'text/plain; charset=us-ascii'
    fnode.write(json.dumps(content))
    fnode.close()
    h5file.flush()


def read_json_node(h5file, *parts):
    path = '/'.join(parts)
    path = _norm_node_path('/' + path)
    if path not in h5file:
        return None

    node = h5file.get_node(path)
    with filenode.open_node(node, 'r') as f:
        content = json.loads(f.read())

    return content


def get_table(db, node_path):
    path = _norm_node_path(node_path)
    return db.get_node(path) if path in db else None


def parse_date(d):
    try:
        return datetime.strptime(d, '%Y-%m-%d')
    except ValueError:
        return None


def _norm_node_path(node_path):
    return node_path.replace('^', 'INDEX_')
