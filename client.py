#!/usr/bin/python

import optparse
import logging

import Pyro.core

from server import *

def main():
    usage = """
        usage: %prog [options]
    """
    
    parser = optparse.OptionParser(usage)
    parser.add_option("-l", "--host", dest="host", default="127.0.0.1", help="Server hostname [default: %default]")
    parser.add_option("-p", "--port", dest="port", default="7766", help="Server port [default: %default]")    
    
    (options, args) = parser.parse_args()
    
    # connect to the manager (single threaded)
    manager = Pyro.core.getProxyForURI("PYROLOC://%s:%s/manager" % (options.host, options.port))
    # get the next replica URI?
    replica_uri = manager.get_next_replica_uri()
    
    if replica_uri is not None:
        replica = replica_uri.getAttrProxy()
        
        print "Got replica:", replica
        replica.status = Replica.RUNNING
        print replica
        # manager.set_replica_status(replica, Replica.RUNNING)
        replica.run()
        replica.status = Replica.FINISHED
        # manager.set_replica_status(replica, Replica.FINISHED)
    else:
        print "Nothing to run!"
        
        
if __name__=='__main__':
    main()