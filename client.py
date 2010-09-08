#!/usr/bin/python
import optparse
import os
import logging
import time
import socket
import Pyro.core
from Pyro.errors import ProtocolError
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
    start_time = time.time()
    
    while(1):
        # connect to the manager
        manager = Pyro.core.getAttrProxyForURI("PYROLOC://%s:%s/manager" % (server_host, server_port))
        try:
            if manager.status == manager.INACTIVE:
                log.info('Manager is inactive, forwarding to new host (%s:%s)' % (manager.server_host, manager.server_port))
                server_host = manager.server_host
                server_port = manager.server_port
            elif manager.status == manager.TRANSFER:
                log.info('Manager wants to transfer to client, starting new server')
                server_path = os.path.abspath(__file__).replace('client.py','server.py')                
                config_path = os.path.abspath(manager.config.filename)
                snapshot_path = os.path.abspath(manager.snapshot.path)
                command = 'python %s -c "%s" -s "%s" -t "%s"' % (server_path, config_path, snapshot_path, str(start_time))
                split_command = shlex.split(command)
                # TODO: logging? signal handling?
                _server_process = subprocess.Popen(split_command)
                log.info('Server is now running on client machine')
                # set the server to inactive
                manager.status = manager.INACTIVE
                manager.server_host = client_ip
                manager.server_port = client_port
            else:
                # manager is active!
                if _run_process is not None:
                    # we need to close this replica if it is still open
                    log.info('Ending run for replica at %s' % replica_uri_path)
                    replica = Pyro.core.getAttrProxyForURI("PYROLOC://%s:%s/%s" % (server_host, server_port, replica_uri_path))
                    replica.end_run(_run_process.poll())
                    _run_process = None
                else:
                    log.info('Manager is active, getting next replica')
                    # returns a string uri
                    replica_uri_path = manager.get_next_replica_uri(options.jobid)
                    if replica_uri_path:
                        log.info('Client got replica uri path: %s' % replica_uri_path)
                        replica = Pyro.core.getAttrProxyForURI("PYROLOC://%s:%s/%s" % (server_host, server_port, replica_uri_path))
                        log.info('Client got replica %s' % str(replica))
                        
                        # run everything here, this may take a while...
                        replica.start_run()
                        log.info('Running replica %s-%s' % (str(replica.id), str(replica.sequence)))
                        log.info('Environment variables: %s' % str(replica.environment_variables()))
                        _run_process = subprocess.Popen(replica.command(), env=replica.environment_variables())
                        _run_process.wait()
                        log.info('Ending run for replica %s-%s' % (str(replica.id), str(replica.sequence)))
                        # if this fails, it will raise ProtocolError which will loop around and connect to a new manager
                        replica.end_run(_run_process.poll())
                        _run_process = None
                    else:
                        log.info('Client did not get any replica to run, will retry...')
        except ProtocolError:
            log.error("Connection to manager failed... will retry")
            tries += 1
            if tries == options.max_retry:
                raise Exception('Maximum number of retries (%d) connecting to server' % options.max_retry)
        else:
            # reset tries on successful connect (no exceptions)
            tries = 0
        finally:
            # always sleep before reconnecting
            time.sleep(2)
        
        
if __name__=='__main__':
    main()