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
    parser.add_option("-c", "--config", dest="config_file", default="config.ini", help="Config file [default: %default]")
    parser.add_option("-l", "--server-host", dest="server_host", default="127.0.0.1", help="Server hostname [default: %default]")
    parser.add_option("-p", "--server-port", dest="server_port", default="7766", help="Server port [default: %default]")
    parser.add_option("-j", "--job-id", dest="job_id", default=str(int(time.time())), help="Job ID [default: %default]")    
    
    (options, args) = parser.parse_args()
    
    config = pydr.setup_config(options.config_file, create=True)
    
    # run the Manager in Pyro
    Pyro.core.initServer()
    daemon = Pyro.core.Daemon(port=int(config['client']['port']))
    client = Client(config, daemon, options.job_id)
    uri = daemon.connect(client, 'client')
        
    log.info('The daemon is running at: %s:%s' % (daemon.hostname, daemon.port))
    log.info('The client\'s uri is: %s' % uri)
    
    try:
        log.debug('Starting client loop...')
        client.connect(options.server_host, options.server_port)
        while 1:
            client.ping()
            daemon.handleRequests(20.0)
    finally:
        daemon.shutdown(True)


class Client(Pyro.core.SynchronizedObjBase):           
    """ The Client object is run from the client, the manager connects to it to run things! """

    def __init__(self, config, daemon, job_id):
        Pyro.core.SynchronizedObjBase.__init__(self)

        self.config = config
        self.client_host = daemon.hostname
        self.client_port = daemon.port
        self.uri = "PYROLOC://%s:%s/client" % (self.client_host, self.client_port)
        if job_id == "": job_id = None
        self.job_id = job_id

        # start_time: basically the time at which the client's job started running.
        #             This is used if/when a server is launched by the client,
        #             to know when the server's walltime will be reached
        self.start_time = time.time()
        
        # if the client launches a server, this is the process it will interact with
        self.server_process = None
        # the process of some program running under this client
        self.run_process = None
        self.replica_uri_path = None
        
        self.connected = False
        self.server_host = None
        self.server_port = None
        self.host_file_path = os.path.join(self.project_path(), self.config['server']['hostfile'])
        if not os.path.exists(self.host_file_path):
            self.host_file_path = None
    
    def project_path(self):
        return os.path.abspath(os.path.dirname(self.config.filename))

    def ping(self):
        """ Connect to the server. The server will tell this client what to do next """        
        if self.run_process is None and self.replica_uri_path is None:
            log.debug('Asking manager for a replica...')
            try:
                # the client may connect to a manager that doesn't serve replicas anymore
                uri = None
                uri = self.manager.get_replica(self.job_id)
            except ProtocolError:
                log.error('Could not connect to manager, will try again...')
            finally:
                if uri is not None:
                    log.debug('running by uri path: %s' % uri)
                    self.run_replica_by_uri_path(uri)
                else:
                    log.debug('no uri response')
                    self.connect()
        elif self.run_process.poll() is not None:
            self.end_running_replica()
                
    def start_server(self, snapshot_path):
        if self.server_process is not None:
            log.error('Manager wants to transfer to client, but server is already running!')
            return False
        else:
            log.info('Manager wants to transfer to client, starting new server')
            server_path = os.path.abspath(__file__).replace('client.py','server.py')                
            config_path = os.path.abspath(self.config.filename)
            # snapshot_path = os.path.abspath(self.manager.snapshot.path)
            command = 'python %s -c "%s" -s "%s" -t "%s"' % (server_path, config_path, snapshot_path, str(self.start_time))
            split_command = shlex.split(command)
            self.server_process = subprocess.Popen(split_command)
            log.info('Server is now running on this node.')
            return True

    def connect(self, host=None, port=None):
        if self.host_file_path:
            f = open(self.host_file_path, 'r')
            line = f.readline()
            f.close()
            (host, port) = line.split(':')
        
        if host == self.server_host and port == self.server_port:
            log.debug('Client already connected to server at %s:%s' % (host, port))
        else:
            log.info('Client switching server to: %s:%s' % (host, port))
            self.server_host = host
            self.server_port = port
        
        log.info('Client connecting to server: %s:%s' % (self.server_host, self.server_port))
        self.manager = Pyro.core.getProxyForURI("PYROLOC://%s:%s/manager" % (self.server_host, self.server_port))
        self.manager._setTimeout(2)
        
        if not self.connected:
            try:
                self.manager.connect(self.job_id, self.uri)
            except ProtocolError:
                log.error("Connection to manager failed, will retry")
            else:
                self.connected = True

    def end_running_replica(self):
        # run process is not None, some job was running and it finished
        # if the job not running anymore... end it
        log.info('Ending run for replica at %s' % self.replica_uri_path)
        self.connect()
        replica = Pyro.core.getAttrProxyForURI("PYROLOC://%s:%s/%s" % (self.server_host, self.server_port, self.replica_uri_path))
        try:            
            if replica.end_run(self.run_process.poll()):
                replica._release()
                self.run_process = None
                self.replica_uri_path = None
        except ProtocolError:
            log.error('Could not connect to replica to end the run, will retry')
    
    def run_replica_by_uri_path(self, replica_uri_path):
        self.replica_uri_path = replica_uri_path
        log.info('Client running replica uri %s' % self.replica_uri_path)
        replica = Pyro.core.getAttrProxyForURI("PYROLOC://%s:%s/%s" % (self.server_host, self.server_port, self.replica_uri_path))
        log.info('Running replica %s-%s' % (str(replica.id), str(replica.sequence)))
        log.info('Environment variables: %s' % str(replica.environment_variables()))
        replica.start_run()
        self.run_process = subprocess.Popen(replica.command(), env=replica.environment_variables())
        replica._release()
   
if __name__=='__main__':
    main()