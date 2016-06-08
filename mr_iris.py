import datetime
import iris
from iris.coords import Cell


class IrisProtocol(object):

    def read(self, line):
        k_str, v_str = line.split('\t', 1)
        return k_str, iris.load_cube(v_str)

    def write(self, key, value):
        return '%s\t%s' % (key, json.dumps(value))


def make_key(c):
    info = {"name": c.standard_name,
            "scalar_coords": {crd.name(): crd.cell(0).point for crd in c.coords(dim_coords=False, dimensions=())}}
    return str(info)


def make_cube(k):
    info = eval(k)
    c = iris.cube.Cube([0], standard_name=info["name"])
    for k, v in info["scalar_coords"].iteritems():
        this_coord = iris.coords.AuxCoord(v, var_name=k)
        c.add_aux_coord(this_coord)
    return c


def passes_contraint(k, con):
    cl = iris.cube.CubeList(make_cube(k))
    excl = cl.extract(con)
    return len(excel) == 1