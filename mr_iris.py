import datetime
from iris.coords import Cell

def make_key(c):
    info = {"name": d.standard_name,
            "scalar_coords": {c.name(): c.cell(0).point for c in d.coords(dim_coords=False, dimensions=())}}
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