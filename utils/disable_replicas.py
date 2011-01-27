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

        disable the replicas with the given ids
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

    print args

    for r_id, r_config in config['replicas'].items():
        if r_id in args:
            sys.stderr.write("disabling replica %s\n" % (r_id))
            config['replicas'][r_id]['enabled'] = False
    
    if options.write:
        f = open(new_config, 'w')
        config.write(f)
        f.close()
    else:
        print config['replicas']
    
if __name__=='__main__':
    main()
