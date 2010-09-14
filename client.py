#!/usr/bin/python
import optparse
import os
import logging
import time
import socket
import Pyro.core
import pydr

import signal
import shlex
import subprocess

# setup logging
log = logging.getLogger("pydr-client")
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch = logging.StreamHandler()
log.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
log.addHandler(ch)

_run_process = None
_server_process = None


def main():
    global _run_process
    global _server_process
    
    usage = """
        usage: %prog [options]
    """
    
    parser = optparse.OptionParser(usage)
    parser.add_option("-l", "--server-host", dest="server_host", default="127.0.0.1", help="Server hostname [default: %default]")
    parser.add_option("-p", "--server-port", dest="server_port", default="7766", help="Server port [default: %default]")
    parser.add_option("-c", "--client-port", dest="client_port", default="7767", help="Server port [default: %default]")    
    # parser.add_option("-f", "--host-file", dest="hostfile", default=None, help="Host file [default: %default]")    
    parser.add_option("-j", "--job-id", dest="job_id", default=str(int(time.time())), help="Job ID [default: %default]")    
    # parser.add_option("-r", "--retry", dest="max_retry", type="int", default=20, help="Max number of connection retries [default: %default]")    
    
    (options, args) = parser.parse_args()
    
    # TODO: look in the hostfile first
    # TODO: get ports from config file
    server_host = options.server_host
    server_port = options.server_port
    
    # get our IP and port
    # client_ip = socket.gethostbyname(socket.gethostname())
    # client_host = socket.gethostname()
    # client_port = options.client_port
    
    # run the Manager in Pyro
    Pyro.core.initServer()
    daemon = Pyro.core.Daemon(port=int(options.client_port))
    client = pydr.Client(daemon, options.job_id)
    uri = daemon.connect(client, 'client')
        
    log.info('The daemon is running at: %s:%s' % (daemon.hostname, daemon.port))
    log.info('The client\'s uri is: %s' % uri)

    try:
        log.debug('Starting client loop...')
        client.set_server(server_host, server_port)
        while 1:
            client.ping_manager()
            daemon.handleRequests(5.0)
            client.run_subprocess()
    finally:
        daemon.shutdown(True)
        
        
if __name__=='__main__':
    main()