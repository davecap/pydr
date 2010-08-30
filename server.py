#!/usr/bin/python

import os
import optparse
import logging
import subprocess
import shutil
import tempfile
import time
from xml.dom.minidom import parseString

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
        # jobs[<job id>] = Job()
        self.jobs = {}
        # replicas[<replica id>] = Replica()
        self.replicas = {}
        
        # TODO: load settings
        for i in range(10):
            r = Replica(id=i, uri=None)
            r.uri = self.daemon.connect(r, 'replica/%d' % r.id)
            self.replicas[r.id] = r
        
        self.submit_all_replicas()
        
    def submit_all_replicas(self):
        """
        Start by submitting a job per replica.
        """

        for r in self.replicas.values():
            j = Job(self)
            j.submit()
            # job id is available after submission
            self.jobs[j.id] = j
            # sleep to prevent overloading the server
            time.sleep(1)
    
    def show_replicas(self):
        """ Print the status of all replicas """
        for r in self.replicas.values():
            logging.info('%s' % r)
     
    def set_replica_status(self, replica, status):
        r = self.replicas[replica.id].status = status

    def get_next_replica_uri(self, jobid):
        """
        When a client is run, it gets a replica URI by calling this function
        """
        logging.info('Job just connected to server!')
        self.show_replicas()
        for r in self.replicas.values():
            if r.status == Replica.READY:
                logging.info('Replica ready: %s' % r)
                
                if jobid and jobid in self.jobs.keys():
                    logging.info('Associating job %s with replica %s' % (jobid, r.id))
                    self.jobs[jobid].replica = r
                    r.job = self.jobs[jobid]
                else:
                    logging.warning('Job sent invalid job id %s, will not associate it with replica' % jobid)
                
                r.status = Replica.SENT
                return r.uri
                # return Pyro.core.PyroURI("PYROLOC://%s:%s/replica/%d" % (self.daemon.host, self.daemon.port, r.id))
        logging.warning('No replicas ready to run')
        return None

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

    # Status
    ERROR = 'error'         # replica sent an error
    RUNNING = 'running'     # replica running
    READY = 'ready'         # replica is waiting to run
    FINISHED = 'finished'   # replica finished running
    SENT = 'sent'           # replica sent to a client

    def __init__(self, id, uri):
        Pyro.core.ObjBase.__init__(self)
        
        self.id = id
        self.status = self.READY
        self.uri = uri
        # current job running this replica
        self.job = None

    def __repr__(self):
        return '<Replica %s:%s>' % (str(self.id), self.status)
    
    def run(self):
        """ The actual code that runs the replica on the client node """
        print 'Running replica %d' % self.id
        return_code = subprocess.call(['sleep', '3'], 0, None, None)
        print 'Replica %s finished' % str(self.id)
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
        logging.info('Submitting job...')
        # note: client will send the jobid back to server to associate a replica with a job
        
        # $PBS_JOBID
        # $PBS_O_WORKDIR

    # def status(self):
    #     """ Get the status of a job. Will determine Node and Replica information if possible """
    #     if self.replica:
    #         return self.replica.status
    #     else:
    #         return 'No replica'

    # Note: PBS-specific code
    # TODO: extending this in the future to allow different queue systems
    
    def submit_job_string(self, job_string):
        """ Submit a job from a string using qsub """

        qsub = 'qsub'

        (f, f_abspath) = tempfile.mkstemp()
        print >>f, job_string
        # print >>f, """
        # 
        # """
        f.close()

        process = subprocess.Popen(qsub+" "+f_abspath, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        returncode = process.returncode
        (out, err) = process.communicate()
        logging.info('Job submit stdout: %s' % out)
        logging.info('Job submit stderr: %s' % err)
        
        # qsub returns "<job_id>.gpc-sched"
        try:
            self.id = message.split('.')[0]
            # if this fails, the job id is invalid
            int(jobid)
        except:
            logging.error('No jobid found in qsub output: %s' % message)
            self.id = None

    def get_job_properties(self):
        if not self.id:
            logging.error('Cannot get job properties with unknown job id')
            return None
        
        process = subprocess.Popen('checkjob --format=XML %s' % (self.id,), shell=True, stdout=PIPE, stderr=PIPE)
        (out,err) = process.communicate()
        if not out:
            return None

        try:
            dom = parseString(out)
            jobs = dom.getElementsByTagName('job')
            job = jobs[0]
            job_props = {}
            for prop in job.attributes.keys():
                job_props[prop]=job.attributes[prop].nodeValue
        except:
            return None
        else:
            return job_props

    def is_job_done(self):
        """ Assume job did start, so if the checkjob never heard of this job it must already have finished. """
        props = self.get_job_properties(self.id)
        if props and props['State'] is not 'Completed':
            return False
        else:
            return True

    def cancel_job(self):
        if not self.id:
            logging.error('Cannot cancel job with unknown job id')
            return
        else:
            logging.info('Cancelling job %s' % self.id)
        
        process = subprocess.Popen('qdel %s' % (self.id,), shell=True, stdout=PIPE, stderr=PIPE)
        (out,err) = process.communicate()


class Node(object):
    """
    The Node class represents a cluster node.
    Using this class you can find out what's happening on a given node.
    """
    pass
      
if __name__=='__main__':
    main()