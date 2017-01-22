from  datetime import datetime

import numpy as np


def update_table(h5file, group, table, serie, cols):
    print 'Updating table /%s/%s (%d values)' % (group, table, len(serie))
    if len(serie) == 0:
        return

    tbl = get_table(h5file, '/{0}/{1}'.format(group, table))
    if tbl is None:
        tbl = h5file.create_table('/' + group, _norm_node_path(table), cols)

    tbl.append(np.rec.array(serie))
    tbl.flush()
    tbl.close()


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
