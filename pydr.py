#!/usr/bin/python
#
# TODO:
#   - DR algorithm for getting next replica to run

import os
import optparse
import logging
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
import signal
from threading import Thread

import Pyro.core
from Pyro.errors import ProtocolError

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
            daemon.handleRequests(10.0)
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
    
    config = setup_config(options.config_file, create=True)
    
    Pyro.core.initServer()
    daemon = Pyro.core.Daemon(port=int(config['server']['port']))
    manager = Manager(config, daemon, options.job_id)
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
                    if run_process.poll() != 0:
                        log.error('Replica returned non-zero code %d!' % run_process.poll())
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

#
# Start of PyDR Library
#

class Manager(Pyro.core.SynchronizedObjBase):
    """
    The Manager class is the class shared by Pyro. It handles jobs, nodes and replicas.
    """

    def __init__(self, config, daemon, job_id):
        Pyro.core.SynchronizedObjBase.__init__(self)
        self.job_id = job_id
        self.config = config
        # manager URI for the hostfile
        self.uri = "PYROLOC://%s:%s/manager" % (daemon.hostname, daemon.port)
        # project path
        self.project_path = os.path.abspath(os.path.dirname(self.config.filename))
        # jobs[<job id>] = Job()
        self.jobs = []
        # replicas[<replica id>] = Replica()
        self.replicas = {}
        # last time a snapshot was made
        self.last_snapshot_time = 0
        # snapshot file path
        self.snapshot_path = os.path.join(self.project_path, self.config['server']['snapshotfile'])
        # details about mobile server
        self.active = False
        # time this is run! (used in maintenance code)
        self.init_time = datetime.datetime.now()
        # host file path
        self.hostfile = os.path.join(self.project_path, self.config['server']['hostfile'])
        # tell the server to shutdown on maintenance if this is true
        self.shutdown = False
    
    def stop(self):
        self.shutdown = True
    
    def start(self):
        """ Start the server """
        log.info('Starting the server!')
        self.active = True
        f = open(self.hostfile, 'w')
        log.debug('Writing hostfile...')
        f.write(self.uri)
        f.close()

        # load from snapshot if one is found
        if os.path.exists(self.snapshot_path):
            log.info('Loading snapshot from %s' % (self.snapshot_path))
            self.snapshot = Snapshot(self.snapshot_path)
            self.jobs = self.snapshot.load_jobs(self)
            self.replicas = self.snapshot.load_replicas(self)
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
            self.snapshot = Snapshot(self.snapshot_path)
            # loop through replicas in the config file
            for r_id, r_config in self.config['replicas'].items():
                log.info('Adding replica: %s -> %s' % (r_id, r_config))
                # create new Replica objects
                r = Replica(manager=self, id=r_id, options=r_config)
                self.replicas[r.id] = r

        if not len(self.replicas.items()):
            raise Exception('No replicas found in config file... exiting!')
        return True

    def maintain(self):
        """ Maintenance code that runs between daemon.handleRequests() calls """
        if not self.active:
            return True

        # always try running autosubmit regardless of timedelta
        self.autosubmit()

        # time since init (how long since the job was started)
        timedelta = datetime.datetime.now()-self.init_time
        # calculate total number of seconds
        seconds_since_start = timedelta.seconds + timedelta.days*24*60*60
        seconds_remaining = self.config['server']['walltime'] - seconds_since_start

        if (seconds_since_start/self.config['server']['snapshottime']) > (self.last_snapshot_time/self.config['server']['snapshottime']):
            # write a snapshot
            self.last_snapshot_time = seconds_since_start
            self.snapshot.save(self)

        active_jobs = [ j for j in self.jobs if not j.completed() and j.id != self.job_id ]
        # log.debug('MAINTENANCE: %d available job(s).' % len(active_jobs))

        # should we submit a new server?
        # if time left is less than half the walltime, submit a new server
        if self.active and seconds_remaining < float(self.config['server']['walltime'])/2.0:
            log.info('MAINTENANCE: Server attempting to transfer...')
            sorted_jobs = sorted(active_jobs, key=lambda j: j.start_time)
            if sorted_jobs == []:
                log.error('MAINTENANCE: No youngest job found for server transfer!')
            else:
                self.snapshot.save(self)
                # try clients from the youngest to oldest
                while len(sorted_jobs) > 0:
                    log.debug('MAINTENANCE: Trying to start server on client %s' % sorted_jobs[-1].id)
                    server_listener = Pyro.core.getProxyForURI(sorted_jobs[-1].uri)
                    server_listener._setTimeout(5)
                    try:
                        started = server_listener.start()
                        if started:
                            log.debug('MAINTENANCE: Started server on client %s' % sorted_jobs[-1].id)
                            self.active = False
                            return
                    except Exception, ex:
                        log.error('MAINTENANCE: Could not connect to youngest client (%s)' % str(ex))
                    # pop the last element and try again
                    server_listener._release()
                    sorted_jobs.pop()
        return True

    def autosubmit(self):
        """ Submit replicas if necessary """
        if not self.config['server']['autosubmit']:
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
            if j.submit():
                self.jobs.append(j)
            else:
                log.warning('Job submission failed, not adding to job list')
            # sleep to prevent overloading the server
            time.sleep(1)

    def show_replicas(self):
        """ Print the status of all replicas """
        for r in self.replicas.values():
            log.info('%s' % r)

    # TODO: maybe also make a dict for this? probably not a big deal at the moment
    def find_job_by_id(self, job_id):
        for j in self.jobs:
            if str(j.id) == str(job_id):
                return j
        return None

    #
    # Client calls to Manager
    #
    
    def connect(self, job_id, listener_uri):
        """ Clients make contact with the server by calling this """
        log.info('Job %s connected.' % str(job_id))        
        job = self.find_job_by_id(job_id)
        # TODO: replace with find_or_create_job??
        if job is None:
            log.error('Job sent unknown job id (%s), creating new job' % job_id)                
            job = Job(self)
            job.id = job_id
            self.jobs.append(job)
        # if this is the first time
        if not job.started:
            job.start()
            job.uri = listener_uri
    
    def get_replica(self, job_id):
        """ Get the next replica for a job to run by calling this """
        log.info('Job %s wants a replica' % str(job_id))        
        job = self.find_job_by_id(job_id)
        if not self.active:
            log.error('Server is not active... will not send the client anything')
        elif job is None:
            log.error('Client with invalid job_id (%s) pinged the server!' % (job_id))
        else:
            self.show_replicas()
            for r in self.replicas.values():
                if r.status == Replica.READY:
                    log.info('Replica ready: %s' % r)
                    job.replica_id = r.id
                    r.job_id = job.id
                    log.info('Sending replica %s to client with job id %s' % (r.id, job.id))
                    r.start_run()
                    return r.environment_variables()
            log.warning('No replicas ready to run')
        return None
    
    def clear_replica(self, replica_id, job_id, return_code):
        """ Clients call this to clear a replica that finished running """
        log.info('Job %s is clearing replica %s' % (str(job_id), str(replica_id)))        
        job = self.find_job_by_id(job_id)
        if job is None:
            log.error('Job %s is invalid!' % str(job_id))
        elif replica_id not in self.replicas:
            log.error('Job %s trying to clear unknown replica %s!' % (str(job_id), str(replica_id)))
        else:
            return self.replicas[replica_id].end_run(return_code)
        return False

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
        self.options.update({'command': self.command(), 'id': str(self.id), 'sequence': str(self.sequence)})
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

python ${pydr_client_path} -j $PBS_JOBID

"""

    def __init__(self, manager):
        self.manager = manager
        self.replica_id = None
        self.id = None
        self.uri = None # the URI of the client listener for this job
        self.start_time = 0
        self.predicted_end_time = 0
        self.started = False
    
    def jobname(self):
        return 'pydrjob'
        
    def start(self):
        self.start_time = datetime.datetime.now()
        # end time should be start time + walltime
        self.predicted_end_time = self.start_time + datetime.timedelta(seconds=float(self.manager.config['client']['walltime']))
        self.started = True
        
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
                        'job_dir': self.manager.project_path,
                        'pydr_client_path': os.path.abspath(os.path.dirname(__file__)),
                        'pydr_server_path': os.path.abspath(os.path.dirname(__file__)),
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
            return False
        else:
            return True

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
        """ See if the job is completed or not """
        return self.started and datetime.datetime.now() >= self.predicted_end_time
        
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

# DR settings
[server]
    port = integer(min=1024, max=65534, default=7766)
    hostfile = string(default='hostfile')
    autosubmit = boolean(default=True)
    # walltime in seconds
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
    main()