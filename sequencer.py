from hadoop.io.SequenceFile import CompressionType
from hadoop.io import BytesWritable, Text
from hadoop.io import SequenceFile
import warnings

import iris
import boto
from boto.s3.key import Key


def get_s3_file(s3_file, fileout, bucket='informatics-emr-mrjob'):
    c = boto.connect_s3()
    boto.s3.connect_to_region('eu-west-1')
    bucket = c.get_bucket(bucket)
    k = bucket.get_key(s3_file)
    with open(fileout, "wb") as f:
        k.get_file(f)
    return fileout 


def write_seq_file(file_out, s3_files_in, tempvaluefile="/tmp/temp.nc"):
    """
    String file path to write to
    A list of string file paths to read from. Each file in is encoded to a
    different k, v pair, with the key equal to the cube's metadata
    
    """
    keys_done = []
    
    writer = SequenceFile.createWriter(file_out, Text, BytesWritable)
    for s3_file_in in s3_files_in:
        f = get_s3_file(s3_file_in, tempvaluefile)
        c = iris.load_cube(f)
        key_writer = Text()
        
        if (str(c.metadata) in keys_done):
            warnings.warn("Key for file "+f+" already present - overwriting")
        key_writer.set(str(c.metadata))
        keys_done.append(str(c.metadata))
        
        value_writer = BytesWritable()
        with open(tempvaluefile, "rb") as f:
            print s3_file_in
            value_writer.set(f.read())
            writer.append(key_writer, value_writer)
    writer.close()