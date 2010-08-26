#!/usr/bin/python

import optparse
import logging
import subprocess
import shutil
import tempfile
from tables import *

import Pyro.core

from dr import *

def main():
    usage = """
        usage: %prog [options]
    """
    
    parser = optparse.OptionParser(usage)
    # parser.add_option("-o", dest="h5_filename", default="analysis.h5", help="Output analysis file [default: %default]")    
    (options, args) = parser.parse_args()
    
    # run the Manager in Pyro
    Pyro.core.initServer()
    daemon = Pyro.core.Daemon()
    
    uri = daemon.connect(Manager(daemon.hostname, daemon.port), "manager")
    print "The daemon runs on port:",daemon.port
    print "The object's uri is:",uri
    daemon.requestLoop()

class Manager(Pyro.core.ObjBase):           
    """
    The Manager class is the class shared by Pyro. It handles jobs, nodes and replicas.
    """
    
    def __init__(self, host, port):
        Pyro.core.ObjBase.__init__(self)
        
        self.host = host
        self.port = port
        
        self.jobs = []
        self.replicas = []
        # TODO: load settings
        for i in range(10):
            self.replicas.append(Replica(id=i))
        self.start()
    
    def show_replicas(self):
        for r in self.replicas:
            print r
        
    def get_next_replica(self):
        """
        When a client is run, it gets a replica to run by calling this function
        """
        print "Job just connected to server!"
        
        self.show_replicas()
        
        for r in self.replicas:
            if r.status == Replica.READY:
                print "Got a replica to run: %s" % r
                return r
        print 'No replicas ready to run'
        return None
        
    def set_replica_status(self, replica, status):
        for r in self.replicas:
            if r.id == replica.id:
                r.status = status
       
    def start(self):
        """
        Start by submitting a job per replica.
        """
        
        for r in self.replicas:
            j = Job(self)
            self.jobs.append(j)
            j.submit()
            # sleep to prevent overloading the server
        
        
if __name__=='__main__':
    main()