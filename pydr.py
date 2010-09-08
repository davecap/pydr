#!/usr/bin/python
#
# TODO:
#   - keep track of state with sqlite or serialize manager object
#   - get the client to run a specific job script
#   - DR algorithm for getting next replica to run

import os
import optparse
import logging
import shlex
import subprocess
import tempfile
import time
import datetime
import math
import pickle
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

class Snapshot(object):
    # TODO: write snapshot
    # what do we need to write:
    #   - jobs (self.jobs)
    #       - job id
    #       - replica id
    #       - job state (get_job_properties()['State'])
    #   - replicas (self.replicas)
    #       - id
    #       - status
    #       - uri
    #       - job
    #       - start_time
    #       - end_time
    #       - sequence
    #   - server information
    #       - status
    #       - host
    #       - ip
    def __init__(self, path):
        self.path = path
        
    def load_jobs(self):
        pass
    
    def load_replicas(self):
        pass
        
    def save(self, manager):
        pass
        
    def close(self):
        pass

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
        
        # jobs[<job id>] = Job()
        self.jobs = []
        # replicas[<replica id>] = Replica()
        self.replicas = {}
        self.config = config
        self.last_snapshot_time = 0     # last time a snapshot was made
        # details about mobile server
        self.status = Manager.ACTIVE
        self.server_host = daemon.hostname
        self.server_port = daemon.port
        
        snapshot_path = os.path.join(self.project_path(), self.config['server']['snapshotfile'])
                
        # load from snapshot if one is found
        if os.path.exists(snapshot_path):
            log.info('Loading snapshot from %s' % (snapshot_path))
            snapshotfile = open(snapshot_path, 'r')
            self.snapshot = pickle.load(snapshotfile)
            snapshotfile.close()
            self.snapshot.path = snapshot_path
            self.jobs = snapshot.load_jobs()
            self.replicas = snapshot.load_replicas()
            for r_id, r_config in self.config['replicas'].items():
                log.info('Validating/Connecting replica: %s' % (r_id))
                if r_id not in self.replicas.keys():
                    raise Exception('Replica %s found in config but not in snapshot!' % str(r_id))
                for c in r_config.keys():
                    if c not in self.replicas[r_id].options.keys():
                        raise Exception('Mismatch between snapshot and config replica options: %s vs %s' % (self.replicas[r_id].options.keys(), r_config.keys()))
            log.info('Snapshot loaded successfuly')
        else:
            # initialize a new snapshot
            self.snapshot = Snapshot(snapshot_path)
            # loop through replicas in the config file
            for r_id, r_config in self.config['replicas'].items():
                log.info('Adding replica: %s -> %s' % (r_id, r_config))
                # create new Replica objects
                r = Replica(manager=self, id=r_id, options=r_config)
                self.replicas[r.id] = r
        
        if not len(self.replicas.items()):
            raise Exception('No replicas found in config file... exiting!')
        else:
            # connect the replicas to the daemon
            for r_id, r in self.replicas.items():
                r.uri_path = 'replica/%s' % r_id
                r.uri = daemon.connect(r, r.uri_path)
    
    def project_path(self):
        return os.path.abspath(os.path.dirname(self.config.filename))
    
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
        
        if (seconds_since_start/self.config['server']['snapshottime']) <= (self.last_snapshot_time/self.config['server']['snapshottime']):
            #log.debug('Not writing a snapshot... snapshottime interval not reached')
            pass
        else:
            self.last_snapshot_time = seconds_since_start
            log.info('Saving server snapshot...')
            self.snapshot.save(self)
            log.info('Done saving server snapshot...')
        
        # should we submit a new server?
        # if time left is less than half the walltime, submit a new server
        if self.status == Manager.ACTIVE and seconds_remaining < float(self.config['server']['walltime'])/2.0:
            log.info('Server will transfer to the next client that connects...')
            self.status = Manager.TRANSFER
            self.snapshot.close()
    
    def autosubmit(self):
        """ Submit replicas if necessary """
        if not self.config['server']['autosubmit']:
            log.debug('Autosubmit is off, not submitting replicas')
            return
        
        # look for any timed-out replicas
        now = datetime.datetime.now()
        for r_id, r in self.replicas.items():
            if r.status == Replica.RUNNING and now >= r.timeout_time:
                log.error('Replica %s timed-out, ending the run' % r_id)
                r.end_run(return_code=1)
                # TODO: check on the job?
        
        # get the number of replicas
        num_replicas = len(self.replicas.keys())
        jobs_running = 0
        for j in self.jobs:
            if not j.completed():
                jobs_running += 1
        
        # if there arent enough jobs for each replica, submit some
        for i in range(num_replicas-jobs_running):
            j = Job(self)
            self.jobs.append(j)
            j.submit()
            # sleep to prevent overloading the server
            time.sleep(1)
                
    def show_replicas(self):
        """ Print the status of all replicas """
        for r in self.replicas.values():
            log.info('%s' % r)
     
    def set_replica_status(self, replica, status):
        r = self.replicas[replica.id].status = status
    
    def find_job_by_id(self, jobid):
        for j in self.jobs:
            if str(j.id) == str(jobid):
                return j
        return None
    
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
                job = self.find_job_by_id(jobid)
                if job:
                    log.info('Associating job %s with replica %s' % (jobid, r.id))
                    job.replica = r
                    job.job_started()
                    r.job = job
                else:
                    log.warning('Job sent invalid job id (%s). Will not associate it with a Replica' % jobid)
                return r.uri_path
        log.warning('No replicas ready to run')
        return None

# Subclass Replica to handle different simulation packages and systems?
class Replica(Pyro.core.ObjBase):
    """
    The Replica class represents an individual replica that is run on a node.
    It contains all the information necessary for a new job to run a replica on a node.
    """

    # Status
    READY = 'ready'         # replica is waiting to run
    RUNNING = 'running'     # replica was sent to a client
    ERROR = 'error'         # replica sent an error
    FINISHED = 'finished'   # replica finished running

    def __init__(self, manager, id, options={}):
        Pyro.core.ObjBase.__init__(self)
        
        self.manager = manager
        self.id = id
        self.status = self.READY
        self.uri = None
        self.sequence = 0
        self.options = options
        self.start_time = None
        self.timeout_time = None
        # current job running this replica
        self.job = None
        
    def __repr__(self):
        return '<Replica %s:%s>' % (str(self.id), self.status)
    
    def start_run(self):
        log.info('Starting run for replica %s-%s (job %s)' % (str(self.id), str(self.sequence), self.job))
        self.start_time = datetime.datetime.now()
        self.timeout_time = self.start_time + datetime.timedelta(seconds=float(self.manager.config['client']['timeout']))
        self.status = self.RUNNING
    
    def end_run(self, return_code=0):
        # TODO: handle return code
        log.info('Ending run for replica %s-%s (job %s)' % (str(self.id), str(self.sequence), self.job))
        self.start_time = None
        self.timeout_time = None
        self.status = self.READY
    
    def environment_variables(self):
        self.options.update({'replica':str(self.id), 'sequence': str(self.sequence)})
        return self.options
    
    def command(self):
        """ Get the actual code that runs the replica on the client node """
        return ['/bin/sh', self.manager.config['client']['files']['run']]

class Job(object):
    """
    The Job class represents a job running through the cluster queue system.
    """
        
    DEFAULT_SUBMIT_SCRIPT_TEMPLATE = """
#!/bin/bash
#PBS -l nodes=${nodes}:ib:ppn=${ppn},walltime=${walltime}${pbs_extra}
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
        self.id = None
    
    def jobname(self):
        return 'pydrjob'
        
    def job_started(self):
        """ Called when a job connects to the server and associates with a Replica """
        self.start_time = datetime.datetime.now()
        # end time should be start time + walltime
        self.predicted_end_time = self.start_time + datetime.timedelta(seconds=float(self.manager.config['client']['walltime']))
        
    def make_submit_script(self, options={}):
        """ Generate a submit script from the template """
        s = Template(self.DEFAULT_SUBMIT_SCRIPT_TEMPLATE)
        defaults = {    'nodes': self.manager.config['client']['nodes'],
                        'ppn': self.manager.config['client']['ppn'],
                        'walltime': self.manager.config['client']['walltime'],
                        #'pbs_extra': self.manager.config['client']['pbs_extra'],
                        'pbs_extra': ['os=centos53computeA'],
                        'jobname': self.jobname(),
                        'mpiflags': self.manager.config['client']['mpiflags'],
                        'job_dir': self.manager.project_path(),
                        'pydr_client_path': os.path.abspath(os.path.dirname(__file__)),
                        'pydr_server_path': os.path.abspath(os.path.dirname(__file__)),
                        'pydr_server': self.manager.server_host,
                        'pydr_port': self.manager.server_port,
                    }
        
        defaults.update(options)
            
        if len(defaults['pbs_extra']) > 0:
            defaults['pbs_extra'] = ','+','.join(defaults['pbs_extra'])
        else:
            defaults['pbs_extra'] = ''
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
        
        # by default jobs start right away... start_time/end_time will be overwritten later
        # TODO: this could be bad!
        self.job_started()
        
        # note: client will send the jobid back to server to associate a replica with a job
        
        # $PBS_JOBID
        # $PBS_O_WORKDIR
        qsub_path = self.manager.config['system']['qsub_path']

        (fd, f_abspath) = tempfile.mkstemp()
        os.write(fd, self.make_submit_script())
        # print >>f, job_string
        # print >>f, """
        # 
        # """
        #print self.make_submit_script()

        log.info('Submit script file: %s' % f_abspath)
        process = subprocess.Popen([qsub_path, f_abspath], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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

    def completed(self):
        """ Assume job did start, so if the checkjob never heard of this job it must already have finished. """
        # look at walltime
        if datetime.datetime.now() >= self.predicted_end_time:
            return True
        else:
            return False
        
        # TODO: look at actual job status
        #   this could be too slow, so for now just look at predicted times
        #props = self.get_job_properties(self.id)
        #if props and props['State'] is not 'Completed':
        #   return False
        #else:
        #   return True

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
    snapshotfile = string(default='snapshot.pickle')

# client (Job) specific information
[client]
    job_name_prefix = string(default='mysystem')
    ppn = integer(min=1, max=64, default=1)
    nodes = integer(min=1, max=9999999, default=1)
    mpiflags = string(default='-mca btl_sm_num_fifos 7 -mca mpi_paffinity_alone 1 -mca btl_openib_eager_limit 32767 -np $(wc -l $PBS_NODEFILE | gawk \'{print $1}\') -machinefile $PBS_NODEFILE')
    # walltime in seconds
    walltime = integer(min=1, max=999999, default=86400)
    # timeout before server resubmits a client
    timeout = integer(min=0, max=999999, default=10000)

    # files required by the client
    [[files]]
        # the run script is executed by the client for each replica
        # it defaults to run.sh (located in the same directory as config.ini)
        # replica variables are passed to this script via the client
        run = string(default='run.sh')
    
        # link files are not modified but are required by each replica/sequence, they are linked to save disk space
        # ex: link = initial.pdb, initial.psf
        link = string_list(min=0, default=list())
        
        # copy files are files copied to each sequence. They may be modified by the run script at run-time
        # copy = md.conf, tclforces.tcl
        copy = string_list(min=0, default=list())
        
        # restart files are output files expected after a replica is done running
        # ex: restart = restart.vel, restart.coor, restart.xsc
        restart = string_list(min=0, default=list())

# Replica settings
[replicas]
    # each replica is listed here numbered from 0 to N
    # [[0]]
    #     k = 0.1
    #     coordinate = 10
    # [[1]]
    #     k = 0.1
    #     coordinate = 20
    # [[2]]
    #     k = 0.1
    #     coordinate = 30

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