#!/usr/bin/python

import optparse
import logging

import Pyro.core

from dr import *

def main():
    usage = """
        usage: %prog [options]
    """
    
    parser = optparse.OptionParser(usage)
    parser.add_option("-l", "--host", dest="host", default="127.0.0.1", help="Server hostname [default: %default]")
    parser.add_option("-p", "--port", dest="port", default="7766", help="Server port [default: %default]")    
    
    (options, args) = parser.parse_args()
    
    manager = Pyro.core.getProxyForURI("PYROLOC://%s:%s/manager" % (options.host, options.port))
    
    replica = manager.get_next_replica()
    if replica:
        manager.set_replica_status(replica, Replica.RUNNING)
        replica.run()
        manager.set_replica_status(replica, Replica.FINISHED)
    else:
        print "Nothing to run!"
        
        
if __name__=='__main__':
    main()