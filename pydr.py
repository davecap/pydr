#!/usr/bin/python
#
# TODO:
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
import copy
from xml.dom.minidom import parseString
from string import Template
from configobj import ConfigObj, flatten_errors
from validate import Validator

import Pyro.core
from Pyro.errors import ProtocolError

# setup logging
log = logging.getLogger("pydr")
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch = logging.StreamHandler()
log.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
log.addHandler(ch)

class Snapshot(object):
    def __init__(self, path):
        """ Load a snapshot data from a pickled Snapshot or just create a new object """
        self.path = path
        if os.path.exists(path):
            f = open(path, 'r')
            snapshot = pickle.load(f)
            f.close()
            self.jobs = snapshot.jobs
            self.replicas = snapshot.replicas            
        
    def load_jobs(self, manager):
        jobs = []
        for j in self.jobs:
            j.manager = manager
            jobs.append(j)
        return jobs
    
    def load_replicas(self, manager):
        replicas = {}
        for r in self.replicas:
            r.manager = manager
            replicas[r.id] = r
        return replicas
            
    def save(self, manager):
        self.replicas = []
        self.jobs = []
        for r in manager.replicas.values():
            r_copy = copy.copy(r)
            # need to remove these references in order top pickle
            r_copy.manager = None
            r_copy.daemon = None
            self.replicas.append(r_copy)
        for j in manager.jobs:
            j_copy = copy.copy(j)
            j_copy.manager = None
            self.jobs.append(j_copy)
        log.debug('Saving snapshot to %s...' % self.path)
        f = open(self.path, 'w')
        pickle.dump(self, f)
        f.close()
        log.debug('Done saving snapshot')

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
        self.job_id = None
        
    def __repr__(self):
        return '<Replica %s:%s>' % (str(self.id), self.status)
    
    def start_run(self):
        log.info('Starting run for replica %s-%s (job %s)' % (str(self.id), str(self.sequence), self.job_id))
        self.start_time = datetime.datetime.now()
        self.timeout_time = self.start_time + datetime.timedelta(seconds=float(self.manager.config['client']['timeout']))
        self.status = self.RUNNING
    
    def end_run(self, return_code=0):
        # TODO: handle return code
        log.info('Ending run for replica %s-%s (job %s)' % (str(self.id), str(self.sequence), self.job_id))
        self.start_time = None
        self.timeout_time = None
        self.status = self.READY
        return True
    
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

# python ${pydr_client_path} -l ${pydr_server} -p ${pydr_port} -j $PBS_JOBID
python ${pydr_client_path} -j $PBS_JOBID

"""

    def __init__(self, manager):
        self.manager = manager
        self.replica_id = None
        self.id = None
        self.uri = None # the URI of the client for this job
        self.start_time = 0
        self.predicted_end_time = 0
    
    def jobname(self):
        return 'pydrjob'
        
    def reset(self, uri):
        self.start_time = datetime.datetime.now()
        # end time should be start time + walltime
        self.predicted_end_time = self.start_time + datetime.timedelta(seconds=float(self.manager.config['client']['walltime']))
        self.uri = uri
        
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
        # self.job_started()
        
        # note: client will send the job_id back to server to associate a replica with a job
        
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
            int(job_id)
        except:
            log.error('No job_id found in qsub output: %s' % out)
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
    hostfile = string(default='hostfile')
    autosubmit = boolean(default=True)
    walltime = integer(min=1, max=999999, default=86400)
    snapshottime = integer(min=1, max=999999, default=3600)
    snapshotfile = string(default='snapshot.pickle')

# client (Job) specific information
[client]
    port = integer(min=1024, max=65534, default=7767)
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

def setup_config(path='config.ini', create=False):
    # validate the config
    config = ConfigObj(path, configspec=PYDR_CONFIG_SPEC.split("\n"))
    validator = Validator()

    # create config file with defaults if necessary
    if create and not os.path.exists(path):
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