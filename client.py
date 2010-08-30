#!/usr/bin/python

import optparse
import logging
import time

import Pyro.core
from Pyro.errors import ProtocolError

from server import *

def main():
    usage = """
        usage: %prog [options]
    """
    
    parser = optparse.OptionParser(usage)
    parser.add_option("-l", "--host", dest="host", default="127.0.0.1", help="Server hostname [default: %default]")
    parser.add_option("-p", "--port", dest="port", default="7766", help="Server port [default: %default]")    
    parser.add_option("-j", "--job-id", dest="jobid", default="", help="Job ID [default: %default]")    
    
    (options, args) = parser.parse_args()
    
    # get manager URI
    manager = Pyro.core.getProxyForURI("PYROLOC://%s:%s/manager" % (options.host, options.port))
    
    tries = 3
    while (1):
        try:
            # connect to manager, give it our jobid if we have one to associate the replica
            replica_uri = manager.get_next_replica_uri(jobid)
        except ProtocolError:
            logging.warning("Connection to manager failed, retrying...")
            time.sleep(2)
            tries -= 1
            if not tries:
                logging.error("Connection to manager failed, quitting!")
                return -1
        else:
            logging.info("Connected to manager")
            break
    
    
    if replica_uri is not None:
        replica = replica_uri.getAttrProxy()
        logging.info("Got replica %s" % replica)
        replica.status = Replica.RUNNING
        # manager.set_replica_status(replica, Replica.RUNNING)
        replica.run()
        replica.status = Replica.FINISHED
        # manager.set_replica_status(replica, Replica.FINISHED)
    else:
        logging.error("Nothing to run!")
        
        
if __name__=='__main__':
    main()