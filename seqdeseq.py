from hadoop.io import BytesWritable, Text
from hadoop.io import SequenceFile
import warnings

import iris
import boto
from boto.s3.key import Key


def sequence(file_out, s3_files_in, make_key, tempvaluefile="/tmp/temp.nc"):
    """
    String file path to write to
    A list of string file paths to read from. Each file in is encoded to a
    different k, v pair, with the key equal to the cube's metadata
    make_key is a function with takes a cube and returns a uid string
    
    """
    keys_done = []
    
    writer = SequenceFile.createWriter(file_out, Text, BytesWritable)
    for s3_file_in in s3_files_in:
        f = get_s3_file(s3_file_in, tempvaluefile)
        c = iris.load_cube(f)
        key_writer = Text()
        
        if (str(c.metadata) in keys_done):
            warnings.warn("Key for file "+f+" already present - overwriting")
        key_writer.set(make_key(c))
        keys_done.append(str(c.metadata))
        
        value_writer = BytesWritable()
        with open(tempvaluefile, "rb") as f:
            print s3_file_in
            value_writer.set(f.read())
            writer.append(key_writer, value_writer)
    writer.close()


def desequence(seq_file, output_path, get_fname=lambda k, i: "file"+str(i)+".nc"):
    """
    Takes a sequence file and writes out a separate NetCDF file
    for each value.

    seq_file: path to a seq file where the values are valid NetCDF binary blobs
    output_path: a string path to dump files to
    get_fname: a function which takes the key and an incrimental integer,
                    and returns a string to be used as the file name.

    """
    reader = SequenceFile.Reader(seq_file)

    key_class = reader.getKeyClass()
    value_class = reader.getValueClass()

    key = key_class()
    value = value_class()

    position = reader.getPosition()
    i = 0
    while reader.next(key, value):
        with open(output_path+get_fname(key, i), "wb") as f:
            f.write(value.getBytes())
        i += 1
    reader.close()


def get_s3_file(s3_file, fileout, bucket='informatics-emr-mrjob'):
    """
    Downloads an S3 file

    """
    c = boto.connect_s3()
    boto.s3.connect_to_region('eu-west-1')
    bucket = c.get_bucket(bucket)
    k = bucket.get_key(s3_file)
    with open(fileout, "wb") as f:
        k.get_file(f)
    return fileout