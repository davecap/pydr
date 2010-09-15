#!/usr/bin/python
import optparse
import os
import logging
import time
import Pyro.core
from Pyro.errors import ProtocolError
import signal
from threading import Thread

import pydr

# setup logging
log = logging.getLogger("pydr")
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch = logging.StreamHandler()
log.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
log.addHandler(ch)

# this is the server listener
# it runs in a separate thread and contains a running server/manager started on command
def server_listener(daemon, manager, start=False):
    log.info('Starting server listener...')
    try:
        if start or not os.path.exists(manager.hostfile):
            manager.start()
        while 1:
            if manager.shutdown:
                break
            manager.maintain()
            daemon.handleRequests(2.0)
    finally:
        log.info('Server shutting down...')
        daemon.shutdown(True)

def main():    
    usage = """
        usage: %prog [options]
    """
    
    parser = optparse.OptionParser(usage)
    parser.add_option("-c", "--config", dest="config_file", default="config.ini", help="Config file [default: %default]")
    parser.add_option("-j", "--job-id", dest="job_id", default=str(int(time.time())), help="Job ID [default: %default]")
    parser.add_option("-s", "--start", dest="start_server", action="store_true", default=False, help="Start the server [default: %default]")    
    
    (options, args) = parser.parse_args()
    
    config = pydr.setup_config(options.config_file, create=True)
    
    Pyro.core.initServer()
    daemon = Pyro.core.Daemon(port=int(config['server']['port']))
    manager = pydr.Manager(config, daemon, options.job_id)
    manager_uri = daemon.connect(manager, 'manager')
    log.info('The daemon is running at: %s:%s' % (daemon.hostname, daemon.port))
    log.info('The manager\'s uri is: %s' % manager_uri)
    
    thread = Thread(target=server_listener, args=(daemon, manager, options.start_server))
    thread.start()
    time.sleep(1)
    
    log.debug('Starting client loop...')
    replica = None
    
    try:
        while True:
            if not thread.is_alive():
                log.error('Listener thread died!')
                break
            # get the server URI from the hostfile
            if not os.path.exists(manager.hostfile):
                log.error('No hostfile exists! will retry...')
                time.sleep(2)
                continue
            f = open(manager.hostfile, 'r')
            server_uri = f.readline()
            f.close()
                        
            # connect to server: job_id, time_left, manager_uri
            server = Pyro.core.getProxyForURI(server_uri)
            server._setTimeout(1)
            try:
                server.connect(options.job_id, manager_uri)
            except ProtocolError:
                log.warning('Could not connect to server, will retry')
            else:
                # see if we need to close a replica that just finished
                if replica is not None:
                    # get the replica by its ID
                    log.info('Ending run for replica %s' % str(replica['id']))
                    # TODO: handle exceptions
                    server.clear_replica(replica_id=replica['id'], job_id=options.job_id, return_code=run_process.poll())
                    replica = None
        
                log.debug('Asking manager for a replica...')
                # TODO: handle exceptions
                replica = server.get_replica(options.job_id)
            
                # if the server returns a replica
                if replica:
                    # res contains the replica environment variables
                    log.info('Client running replica %s' % replica['id'])
                    log.info('Environment variables: %s' % replica)
                    # run the replica
                    # TODO: clean this up
                    command = replica['command']
                    del replica['command']
                    run_process = subprocess.Popen(command, env=replica)
                    # now just wait until the job is done
                    run_process.wait()
                else:
                    log.error('Client did not get a replica from the server')
            finally:
                time.sleep(2)
        # end client loop
    finally:
        manager.stop()
        thread.join(1)
   
if __name__=='__main__':
    main()