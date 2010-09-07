#!/usr/bin/python
import optparse
import os
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
    replica_uri_path = None
    tries = 0
    
    log.debug('Starting client loop...')
    while(1):
        # connect to the manager
        manager = Pyro.core.getAttrProxyForURI("PYROLOC://%s:%s/manager" % (server_host, server_port))
        try:
            if manager.status == manager.INACTIVE:
                log.info('Manager is inactive, getting new host/port')
                server_host = manager.server_host
                server_port = manager.server_port
            elif manager.status == manager.TRANSFER:
                log.info('Manager wants to transfer to client, starting new manager')
                # set the server to inactive
                manager.status = manager.INACTIVE
                manager.server_host = client_host
                manager.server_port = client_port
                client_path = os.path.abspath(os.path.dirname(__file__)),
                server_path = client_path.replace('client.py','server.py')
                # TODO
                
                #config_path = os.path.abspath(manager.config.filename)
                #snapshot_path = os.path.abspath(manager.snapshot.path)
                #'python ${server_path} -c -s'
            else:
                if manager.server_host != server_host:
                    log.info('Manager is forwarding this client to new host (%s:%s)' % (manager.server_host, manager.server_port))
                    server_host = manager.server_host
                    server_port = manager.server_port
                else:
                    log.info('Manager is active, getting next replica')
                    # returns a string uri
                    replica_uri_path = manager.get_next_replica_uri(options.jobid)
                    if replica_uri_path:
                        log.info('Client got replica uri path: %s' % replica_uri_path)
                        replica = Pyro.core.getAttrProxyForURI("PYROLOC://%s:%s/%s" % (server_host, server_port, replica_uri_path))
                        log.info('Client got replica %s' % str(replica))
                        # run everything here, this may take a while...
                        replica.run()
                    else:
                        log.info('Client did not get any replica to run, will retry...')              
        except ProtocolError:
            log.error("Connection to manager failed... will retry")
            tries += 1
            if tries > options.max_retry:
                raise Exception('Maximum number of retries (%d) connecting to server' % options.max_retry)
        time.sleep(15)
        
        
if __name__=='__main__':
    main()