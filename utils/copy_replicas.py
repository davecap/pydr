#!/usr/bin/python

import os
import sys
import shutil
import optparse
from configobj import ConfigObj, flatten_errors
import tables
from math import pi
import tempfile
import random

def main():    
    usage = """
        usage: %prog [options] <replica id> <replica id> ...

        copy the replicas with the given ids
    """
    
    parser = optparse.OptionParser(usage)
    parser.add_option("-c", "--config", dest="config_file", default="config.ini", help="Config file [default: %default]")
    parser.add_option("-w", "--write", dest="write", default=False, action="store_true", help="Write new config file [default: %default]")
    
    (options, args) = parser.parse_args()

    if not os.path.exists(options.config_file):
        raise Exception('No config file found!')
    
    new_config = options.config_file+'.new'
    config = ConfigObj(options.config_file)
    project_path = os.path.abspath(os.path.dirname(config.filename))

    ids = [ int(i) for i in config['replicas'].keys() ]
    next_id = str(max(ids) + 1)

    for r_id in args:
        sys.stderr.write("copying replica %s to %s\n" % (r_id, next_id))
        prev_dir = os.path.join(project_path, r_id)
        new_dir = os.path.join(project_path, next_id)
        if options.write:
            os.mkdir(new_dir)
            shutil.copy(os.path.join(prev_dir, "restart.tar.gz"), new_dir)
        config['replicas'][next_id] = config['replicas'][r_id]
        next_id = str(int(next_id) + 1)

    if options.write:
        f = open(new_config, 'w')
        config.write(f)
        f.close()
    
if __name__=='__main__':
    main()
