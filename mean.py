'''
Takes daily fields and calculates monthly climatologies

'''

from mrjob.job import MRJob
import iris
import mr_iris
import numpy as np


class MRConstraints(MRJob):
    def match_dim(self, k, d):
        k_names = [c.var_name for c in k.coords()]
        overlap = list(filter(lamda x: x in k_names, self.get_names(d)))
        if len(overlap) < 1:
            raise KeyError(d + " dimension not found")
        return overlap[0]

    def get_names(self, opt):
        return getattr(self.options, opt+'-dim-name').split(',')

    def get_range(self, opt):
        return getattr(self.options, opt+'-range').split(',')

    def get_constraints(self, key, dim_names):
        cons = {}
        for dname in dim_names:
            dim = self.match_dim(key, dname)
            minv, maxv = self.get_range(dim)
            cons[dim] = lambda c: minv < c < maxv
        return cons


class MRFilterSlices(MRConstraints):
    def configure_options(self):
        super(MRPdf, self).configure_options()
        self.add_passthrough_option(
            '--time-dim-name', type='string', default="time", help='comma separated list of names (default=time)')
        self.add_passthrough_option(
            '--alt-dim-name', type='string', default="height", help='comma separated list of names (default=height)')
        self.add_passthrough_option(
            '--time-range', type='string', default="", help='comma separated max and min values (defaults to all)')
        self.add_passthrough_option(
            '--alt-range', type='string', default="", help='comma separated max and min values (defaults to all)')
        self.add_passthrough_option(
            '--time-unit', type='string', default="", help='comma separated list of units (defaults to all)')
        self.add_passthrough_option(
            '--alt-unit', type='string', default="", help='comma separated list of units (defaults to all)')

    def mapper(self, key, data):
        try:
            con = self.get_constraints(key, ['alt', 'time'])
            if mr_iris.passes_constraint(key, con):
                yield(key, data)
        except KeyError:
            pass


class MRSubset(MRConstraints):
    def configure_options(self):
        super(MRPdf, self).configure_options()
        self.add_passthrough_option(
            '--lat-dim-name', type='string', default="latitude", help='comma separated list of names (default=latitude)')
        self.add_passthrough_option(
            '--lon-dim-name', type='string', default="longitude", help='comma separated list of names (default=longitude)')
        self.add_passthrough_option(
            '--lat-range', type='string', default="", help='comma separated max and min values (defaults to all)')
        self.add_passthrough_option(
            '--lon-range', type='string', default="", help='comma separated max and min values (defaults to all)')

    def mapper(self, key, data):
        key_cube = mr_iris.make_cube(key)
        data_cube = iris.load_cube(data)
        try:
            values = self.get_constraints(key, ['lat', 'lon'])
            if mr_iris.passes_constraint(key, values):
                data_cube = data_cube.extract(iris.Constraint(coord_values=values))
                keyout = mr_iris.make_key(data_cube)
                yield(keyout, data_cube)
        except KeyError:
            pass


class MRMean(MRConstraints):
    def configure_options(self):
        super(MRPdf, self).configure_options()
        self.add_passthrough_option(
            '--mean-dim-name', type='string', default="", help='name of dimension to average over')

    def mapper(self, key, data):
        key_cube = mr_iris.make_cube(key)
        data_cube = iris.load_cube(data)
        try:
            mean_dim = self.match_dim(key_cube, 'mean')
            cons = data_cube.extract(iris.Constraint(mean_dim))
            mean = cons.sum() / cons.count()
            yield()
        except KeyError:
            pass

    def reducer(self, season, histos):



if __name__ == '__main__':
     MRMean.run()