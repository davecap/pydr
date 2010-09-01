#!/usr/bin/python
import optparse
import logging
import time
import socket
import Pyro.core
from Pyro.errors import ProtocolError
import pydr

# setup logging
log = logging.getLogger("pydr-client")
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch = logging.StreamHandler()
log.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
log.addHandler(ch)

def main():
    usage = """
        usage: %prog [options]
    """
    
    parser = optparse.OptionParser(usage)
    parser.add_option("-l", "--host", dest="host", default="127.0.0.1", help="Server hostname [default: %default]")
    parser.add_option("-p", "--port", dest="port", default="7766", help="Server port [default: %default]")    
    parser.add_option("-f", "--host-file", dest="hostfile", default=None, help="Host file [default: %default]")    
    parser.add_option("-j", "--job-id", dest="jobid", default="", help="Job ID [default: %default]")    
    parser.add_option("-r", "--retry", dest="max_retry", type="int", default=20, help="Max number of connection retries [default: %default]")    
    
    (options, args) = parser.parse_args()
    
    # TODO: look in the hostfile first
    server_host = options.host
    server_port = options.port
    
    # get our IP and port
    client_ip = socket.gethostbyname(socket.gethostname())
    client_host = socket.gethostname()
    client_port = options.port
    
    # for the duration of this job, run a while loop that repeatedly gets runs from the server
    tries = 0
    log.debug('Starting client loop...')
    while(1):
        # connect to the manager
        manager = Pyro.core.getAttrProxyForURI("PYROLOC://%s:%s/manager" % (server_host, server_port))
        
        try:
            if manager.status == manager.INACTIVE:
                log.info('Manager is inactive, getting new host/port')
                server_host = manager.new_server_host
                server_port = manager.new_server_port
            elif manager.status == manager.TRANSFER:
                log.info('Manager wants to transfer to client, starting new manager')
                # set the server to inactive
                manager.status = manager.INACTIVE
                manager.new_server_host = client_host
                manager.new_server_port = client_port
                # TODO: start a new server here
            else:
                log.info('Manager is active, getting next replica')
                replica_uri = manager.get_next_replica_uri(options.jobid)
                if replica_uri is not None:
                    replica = replica_uri.getAttrProxy()
                    log.info("Got replica %s" % replica)
                    replica.status = Replica.RUNNING
                    replica.run()
                    replica.status = Replica.FINISHED
                else:
                    log.warning("Nothing to run... retrying")
        
        except ProtocolError:
            log.error("Connection to manager failed... will retry")
            tries += 1
            if tries > options.max_retry:
                raise Exception('Maximum number of retries (%d) connecting to server' % options.max_retry)
        time.sleep(15)
        
        
if __name__=='__main__':
    main()