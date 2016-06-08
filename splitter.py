import iris
import iris.quickplot as qplt
import matplotlib.pyplot as plt
import boto
from boto.s3.key import Key

from tempfile import mkstemp

def get_name(cube):
    return cube.name()

def splitter(fnames, opendap, cons, latname, lonname):
    c = boto.connect_s3()
    boto.s3.connect_to_region('eu-west-1')
    bucket = c.get_bucket('informatics-emr-mrjob')
    n = 0
    for fname in fnames:
        d = iris.load_cube(opendap+fname, cons)
        for subd in d.slices([lonname, latname]):
            print n,
            _, fname = mkstemp(suffix="nc")
            iris.save(subd, fname, saver='nc', netcdf_format='NETCDF4')
            k = Key(bucket)
            k.key = "rawdata/"+ get_name(subd) + str(n) + ".nc"
            k.set_contents_from_filename(fname)
            n+=1