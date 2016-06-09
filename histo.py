'''
Takes daily fields and calculates monthly climatologies

'''

from mrjob.job import MRJob
import iris
import mr_iris
import numpy as np


class MRHisto(MRJob):

    def mapper(self, key, data):
        c = mr_iris.make_cube(key)
        month = c.coord("time").cell(0).point.month
        if month not in [12, 1, 2] # djf
            d = iris.load_cube(data)
            histo = np.histogram(d.data.data[np.logical_not(d.data.mask)].flatten(), bins=(range(230, 320, 10)))
            keyout = ""
            if m in [3, 4, 5]:
                keyout = "MAM"
            elif m in [6, 7, 8]:
                keyout = "JJA"
            elif m in [9, 10, 11]:
                keyout = "SON"
            yield (keyout, histo)

    def reducer(self, season, histos):
        yield (season, reduce(lambda p, q: p+q, histos))


if __name__ == '__main__':
     MRHisto.run()