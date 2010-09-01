#!/usr/bin/python
# TODO:
#   - keep track of state with sqlite or serialize manager object
#   - output snapshots every N seconds
#   - get the client to run a specific job script
#   - DR algorithm for getting next replica to run

import os
import optparse
import logging
import subprocess
import tempfile
import time
import datetime
import math
from xml.dom.minidom import parseString
from string import Template
from configobj import ConfigObj, flatten_errors
from validate import Validator

import Pyro.core

# setup logging
log = logging.getLogger("pydr")
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch = logging.StreamHandler()
log.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
log.addHandler(ch)

class Manager(Pyro.core.SynchronizedObjBase):           
    """
    The Manager class is the class shared by Pyro. It handles jobs, nodes and replicas.
    """
    
    # manager status
    ACTIVE = 1      # server is running
    INACTIVE = 3    # server is inactive, send clients to new server
    TRANSFER = 2    # server is ready to transfer to new machine
    
    def __init__(self, config, daemon):
        Pyro.core.SynchronizedObjBase.__init__(self)
        
        self.daemon = daemon
        # jobs[<job id>] = Job()
        self.jobs = {}
        # replicas[<replica id>] = Replica()
        self.replicas = {}
        self.config = config
        self.last_snapshot_time = 0     # last time a snapshot was made
        
        # details about mobile server
        self.status = Manager.ACTIVE
        self.new_server_host = None
        self.new_server_port = None
        
        # configs: system, server, jobs, simulation, replicas
        
        for r_id, r_config in self.config['replicas'].items():
            log.info('Adding replica: %s -> %s' % (r_id, r_config))
            r = Replica(id=r_id, options=r_config)
            r.uri = self.daemon.connect(r, 'replica/%s' % r.id)
            self.replicas[r.id] = r
        
        if not len(self.replicas.items()):
            raise Exception('No replicas found in config file... exiting!')
    
    def maintain(self, timedelta):
        """ Maintenance code that runs between daemon.handleRequests() calls """
        if self.status == Manager.TRANSFER:
            log.debug('Skipping maintenance, in transfer mode...')
            return
        
        # always try running autosubmit regardless of timedelta
        self.autosubmit()
        
        # calculate total number of seconds
        seconds_since_start = timedelta.seconds + timedelta.days*24*60*60
        seconds_remaining = self.config['server']['walltime'] - seconds_since_start
        
        self.snapshot(seconds_since_start)
        
        # should we submit a new server?
        # if time left is less than half the walltime, submit a new server
        if self.status == Manager.ACTIVE and seconds_remaining < float(self.config['server']['walltime'])/2.0:
            log.info('Server will transfer to the next client that connects...')
            self.status = Manager.TRANSFER
            # TODO: write/flush DB
    
    def snapshot(self, seconds_since_start=0):
        """ Write a snapshot of the Manager to disk """
        if (seconds_since_start/self.config['server']['snapshottime']) <= (self.last_snapshot_time/self.config['server']['snapshottime']):
            log.debug('Not writing a snapshot... snapshottime interval not reached')
            return False
        
        self.last_snapshot_time = seconds_since_start
        log.info('Writing server snapshot...')
        # TODO: write snapshot
       
    def autosubmit(self):
        """
        Submit replicas 
        """
        if not self.config['server']['autosubmit']:
            log.debug('Autosubmit is off, not submitting replicas')
            return
        
        for r in self.replicas.values():
            if r.status == Replica.IDLE:
                j = Job(self)
                j.submit()
                # job id is available after submission
                if j.id:
                    self.jobs[j.id] = j
                # sleep to prevent overloading the server
                time.sleep(1)
                r.status = Replica.READY
    
    def save_state(self):
        # TODO
        pass
    
    def show_replicas(self):
        """ Print the status of all replicas """
        for r in self.replicas.values():
            log.info('%s' % r)
     
    def set_replica_status(self, replica, status):
        r = self.replicas[replica.id].status = status
    
    #
    # Client calls to Manager
    #

    def get_next_replica_uri(self, jobid):
        """ When a client is run, it gets a replica URI by calling this function """
        log.info('Job just connected to server!')
        self.show_replicas()
        for r in self.replicas.values():
            if r.status == Replica.READY:
                log.info('Replica ready: %s' % r)
                if jobid and jobid in self.jobs.keys():
                    log.info('Associating job %s with replica %s' % (jobid, r.id))
                    self.jobs[jobid].replica = r
                    r.job = self.jobs[jobid]
                else:
                    log.warning('Job sent invalid job id %s, will not associate it with replica' % jobid)
                # r.status == Replica.RUNNING # status set in client
                return r.uri
                # return Pyro.core.PyroURI("PYROLOC://%s:%s/replica/%d" % (self.daemon.host, self.daemon.port, r.id))
        log.warning('No replicas ready to run')
        return None

# Subclass Replica to handle different simulation packages and systems?
class Replica(Pyro.core.ObjBase):
    """
    The Replica class represents an individual replica that is run on a node.
    It contains all the information necessary for a new job to run a replica on a node.

    	unsigned int sample_count;
    	unsigned int sampling_runs;
    	unsigned int sampling_steps;
    	double cancellation_accumulator[2];
    	unsigned short cancellation_count;
    	float cancellation_energy;
    	struct buffer_struct restart;
    	struct atom_struct *atom;
    	unsigned int *presence;
    	char vREfile[500];
    	int nodeSlot;
    """

    # Status
    IDLE = 'idle'     # replica is idle, not involved in anything yet
    READY = 'ready'         # replica is waiting to run
    RUNNING = 'running'     # replica was sent to a client
    ERROR = 'error'         # replica sent an error
    FINISHED = 'finished'   # replica finished running

    def __init__(self, id, options={}):
        Pyro.core.ObjBase.__init__(self)
        
        self.id = id
        self.status = self.IDLE
        self.uri = None
        # current job running this replica
        self.job = None
        self.start_time = None
        self.end_time = None
        
        # settings will come in **kwargs
        # print options

    def __repr__(self):
        return '<Replica %s:%s>' % (str(self.id), self.status)
    
    def run(self):
        """ The actual code that runs the replica on the client node """
        print 'Running replica %d' % self.id
        self.start_time = datetime.datetime.now()
        self.end_time = None
        # TODO: what to do here??
        
        return_code = subprocess.call(['sleep', '3'], 0, None, None)
        print 'Replica %s finished' % str(self.id)
        self.end_time = datetime.datetime.now()
        return return_code

class Job(object):
    """
    The Job class represents a job running through the cluster queue system.
    """
        
    DEFAULT_SUBMIT_SCRIPT_TEMPLATE = """
#!/bin/bash
#PBS -l nodes=${nodes}:ib:ppn=${ppn},walltime=${walltime},${extra}
#PBS -N ${jobname}

MPIFLAGS="${mpiflags}"

# $PBS_O_WORKDIR
# $PBS_JOBID

cd $job_dir

python ${pydr_client_path} -l ${pydr_server} -p ${pydr_port} -j $PBS_JOBID

"""

    def __init__(self, manager):
        self.manager = manager
        self.replica = None
        self.node = None
        self.id = None
    
    def jobname(self):
        return 'pydrjob'
        
    def make_submit_script(self, options={}):
        s = Template(self.DEFAULT_SUBMIT_SCRIPT_TEMPLATE)
        defaults = {    'nodes': '1',
                        'ppn': '8',
                        'walltime': '86400',
                        'extra': ['os=centos53computeA'],
                        'jobname': self.jobname(),
                        'mpiflags': '',
                        'job_dir': os.path.abspath(os.path.dirname(self.manager.config.filename)),
                        'pydr_client_path': os.path.abspath(os.path.dirname(__file__)),
                        'pydr_server': self.manager.daemon.hostname,
                        'pydr_port': self.manager.daemon.port,
                    }
        
        defaults.update(options)
            
        if len(defaults['extra']) > 0:
            defaults['extra'] = ','+','.join(defaults['extra'])
        else:
            defaults['extra'] = ''
        defaults['pydr_client_path'] = os.path.join(defaults['pydr_client_path'], 'client.py')
        
        # have to format walltime from seconds to HH:MM:SS
        # TODO: this is horrible... I couldn't figure out how to get this done properly with python
        t = datetime.timedelta(seconds=int(defaults['walltime']))
        if t.days > 0:
            hours = int(str(t).split(' ')[2].split(':')[0]) + (t.days*24)
            mins = str(t).split(' ')[2].split(':')[1]
            secs = str(t).split(' ')[2].split(':')[2]
            defaults['walltime'] = '%s:%s:%s' % (str(hours), mins, secs)
        else:
            defaults['walltime'] = str(t)
        
        return s.safe_substitute(defaults)
    
    # Note: PBS-specific code
    # TODO: extending this in the future to allow different queue systems
    
    def submit(self):
        """ Submit a job using qsub """
        
        log.info('Submitting job...')
        # note: client will send the jobid back to server to associate a replica with a job
        
        # $PBS_JOBID
        # $PBS_O_WORKDIR
        qsub = 'qsub'

        (fd, f_abspath) = tempfile.mkstemp()
        os.write(fd, self.make_submit_script())
        # print >>f, job_string
        # print >>f, """
        # 
        # """
        #print self.make_submit_script()

        log.info('Submit script file: %s' % f_abspath)

        process = subprocess.Popen(qsub+" "+f_abspath, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        returncode = process.returncode
        (out, err) = process.communicate()
        log.info('Job submit stdout: %s' % out)
        log.info('Job submit stderr: %s' % err)
        
        # qsub returns "<job_id>.gpc-sched"
        try:
            self.id = out.split('.')[0]
            # if this fails, the job id is invalid
            int(jobid)
        except:
            log.error('No jobid found in qsub output: %s' % out)
            self.id = None

    def get_job_properties(self):
        if not self.id:
            log.error('Cannot get job properties with unknown job id')
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
            log.error('Cannot cancel job with unknown job id')
            return
        else:
            log.info('Cancelling job %s' % self.id)
        
        process = subprocess.Popen('qdel %s' % (self.id,), shell=True, stdout=PIPE, stderr=PIPE)
        (out,err) = process.communicate()

#
# Config Spec
#

PYDR_CONFIG_SPEC = """#
# PyDR Config File
#

# set a title for this setup
title = string(default='My DR')

# System paths
[system]
qsub_path = string(default='qsub')
checkjob_path = string(default='checkjob')

# DR server settings
[server]
port = integer(min=1024, max=65534, default=7766)
autosubmit = boolean(default=True)
walltime = integer(min=1, max=999999, default=86400)
snapshottime = integer(min=1, max=999999, default=3600)

# client (Job) specific information
[client]
job_name_prefix = string(default='mysystem')
ppn = integer(min=1, max=64, default=1)
nodes = integer(min=1, max=9999999, default=1)
# walltime in seconds
walltime = integer(min=1, max=999999, default=86400)
# timeout before server resubmits a client
timeout = integer(min=0, max=999999, default=10000)

# TODO: simulation type?
# TODO: extra parameters depending on type will go here

# DRPE settings (simulation -> drpe)
[drpe]

# Replica settings
[replicas]

# END
"""

def setup_config(path='config.ini'):
    # validate the config
    config = ConfigObj(path, configspec=PYDR_CONFIG_SPEC.split("\n"))
    validator = Validator()

    # create config file with defaults if necessary
    if not os.path.exists(path):
        log.info('Creating new config file: %s' % path)
        config.validate(validator, copy=True)
        config.filename = path
        config.write()
    else:
        result = config.validate(validator, preserve_errors=True)
        # show config errors if there are any
        if type(result) is dict:
            for entry in flatten_errors(config, result):
                section_list, key, error = entry
                if key is not None:
                   section_list.append(key)
                else:
                    section_list.append('[missing section]')
                section_string = ' -> '.join(section_list)
                if error == False:
                    error = 'Missing value or section.'
                print section_string, ' = ', error
            raise Exception('Errors in config file')
    return config
            
if __name__=='__main__':
    print "This is the pydr library"