#!/usr/bin/python

import optparse
import logging
import subprocess
import shutil
import tempfile
from tables import *

import Pyro.core

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
    
    uri = daemon.connect(Manager(daemon), "manager")
    print "The daemon runs on port:",daemon.port
    print "The object's uri is:",uri
    daemon.requestLoop()

class Manager(Pyro.core.SynchronizedObjBase):           
    """
    The Manager class is the class shared by Pyro. It handles jobs, nodes and replicas.
    """
    
    def __init__(self, daemon):
        Pyro.core.SynchronizedObjBase.__init__(self)
        
        self.daemon = daemon
        
        self.jobs = []
        self.replicas = []
        # TODO: load settings
        for i in range(10):
            r = Replica(id=i, uri=None)
            r.uri = self.daemon.connect(r, 'replica/%d' % r.id)
            self.replicas.append(r)
        self.start()
    
    def show_replicas(self):
        for r in self.replicas:
            print r
        
    def get_next_replica_uri(self):
        """
        When a client is run, it gets a replica to run by calling this function
        """
        print "Job just connected to server!"
        
        self.show_replicas()
        
        for r in self.replicas:
            if r.status == Replica.READY:
                print "Got a replica to run: %s" % r
                r.status = Replica.SENT
                return r.uri
                # return Pyro.core.PyroURI("PYROLOC://%s:%s/replica/%d" % (self.daemon.host, self.daemon.port, r.id))
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

# Subclass Replica to handle different simulation packages and systems?
class Replica(Pyro.core.ObjBase):
    """
    The Replica class represents an individual replica that is run on a node.
    It contains all the information necessary for a new job to run a replica on a node.

    Original struct replica_struct in DR:
        char status;
    	float w;
    	float w_nominal;
    	float w_start;
    	float w2_nominal;
    	float w_sorted;
    	float force;
    	unsigned int sequence_number;
    	unsigned int sample_count;
    	unsigned int sampling_runs;
    	unsigned int sampling_steps;
    	double cancellation_accumulator[2];
    	unsigned short cancellation_count;
    	float cancellation_energy;
    	unsigned int last_activity_time;
    	unsigned int start_time_on_current_node;
    	struct buffer_struct restart;
    	struct atom_struct *atom;
    	unsigned int *presence;
    	char vREfile[500];
    	int nodeSlot;

    """

    ERROR = 'error'
    RUNNING = 'running'
    READY = 'ready'
    FINISHED = 'finished'
    SENT = 'sent' # replica sent to a client

    def __init__(self, id, uri):
        Pyro.core.ObjBase.__init__(self)
        
        self.id = id
        self.status = self.READY
        self.uri = uri

    def __repr__(self):
        return '<Replica %d:%s>' % (self.id, self.status)

    def run(self):
        """ The actual code that runs the replica on the client node """
        print 'Running replica %d' % self.id
        return_code = subprocess.call(['sleep', '3'], 0, None, None)
        print 'Replica %d finished' % self.id
        return return_code


class Job(object):
    """
    The Job class represents a job running through the cluster queue system.
    """

    def __init__(self, manager):
        self.manager = manager
        self.replica = None
        self.node = None

    def submit(self):
        """ Submit the job to a node by using qsub """
        print 'Submitting job...'

    def status(self):
        """ Get the status of a job. Will determine Node and Replica information if possible """
        if self.replica:
            return self.replica.status
        else:
            return 'No replica'


class Node(object):
    """
    The Node class represents a cluster node.
    Using this class you can find out what's happening on a given node.
    """
    pass
      
if __name__=='__main__':
    main()