#!/usr/bin/python

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
log.setLevel(logging.INFO)
ch.setFormatter(formatter)
log.addHandler(ch)

# this is the server listener
# it runs in a separate thread and contains a running server/manager started on command
def server_listener(daemon, manager, start=False, reset=False):
    log.info('Starting server listener...')
    try:
        if start:
            manager.start()
            if reset:
                manager.force_reset()
        while 1:
            if manager.shutdown:
                break
            manager.maintain()
            daemon.handleRequests(20.0)
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
    parser.add_option("-n", "--pbs-nodefile", dest="pbs_nodefile", default=None, help="Contents of $PBS_NODEFILE")
    parser.add_option("--reset", dest="reset", default=False, action="store_true", help="Restart all replicas and jobs by resetting their status")
    
    (options, args) = parser.parse_args()
    
    config = setup_config(options.config_file, create=True)
    
    if config['debug']:
        log.setLevel(logging.DEBUG)
        log.debug('Debug mode is on!')
    
    Pyro.core.initServer()
    daemon = Pyro.core.Daemon(port=int(config['manager']['port']))
    manager = Manager(config, daemon, options.job_id)
    manager_uri = daemon.connect(manager, 'manager')
    log.info('The daemon is running at: %s:%s' % (daemon.hostname, daemon.port))
    log.info('The manager\'s uri is: %s' % manager_uri)
    
    thread = Thread(target=server_listener, args=(daemon, manager, options.start_server, options.reset))
    thread.start()
    
    log.debug('Starting client loop...')
    replica = None
    tries = 0
    
    try:
        while True:
            if not thread.is_alive():
                log.error('Listener thread died!')
                break
            
            # sleep some time first and before next connection
            time.sleep(10)
            # the manager's Pyro URI is in a hostfile
            # first look for the file...
            if not os.path.exists(manager.hostfile):
                log.error('No hostfile exists! will retry...')
                time.sleep(5)
                continue
            else:
                # hostfile is found, read it and get the server's Pyro URI
                f = open(manager.hostfile, 'r')
                server_uri = f.readline()
                f.close()
                # connect to server: job_id, time_left, manager_uri
                server = Pyro.core.getProxyForURI(server_uri)
                server._setTimeout(1)
        
            try:
                # try connecting to the server, let it know that this job has started!
                server.connect(options.job_id, manager_uri)
            except ProtocolError:
                # can't connect to the server, will try 10 times in a row before quitting
                tries += 1
                # TODO: max_connect_retry config parameter
                if tries == 10:
                    log.error('Could not connect to server 10 times, quitting!')
                    break
            else:
                # client can connect to the server
                tries = 0 # reset try count
            
                # first, see if we need to close a replica that just finished
                if replica is not None:
                    # the actual replica variable contains a 2-tuple
                    (command, env) = replica
                    # get the replica by its ID
                    log.info('Ending run for replica %s' % str(env['id']))
                    try:
                        server.end_replica(replica_id=env['id'], job_id=options.job_id, return_code=run_process.poll())
                    except ProtocolError:
                        log.error('end_replica manager call failed!')
                    replica = None
    
                # by this time, we aren't running any replicas so we ask for a new one
                log.debug('Asking manager for a replica...')
                try:
                    replica = server.get_replica(options.job_id, options.pbs_nodefile)
                except ProtocolError:
                    log.error('get_replica call failed!')
                    replica = None
            
                # check that the server returns a replica
                if replica is not None:
                    (command, env) = replica
                    # res contains the replica environment variables
                    log.info('Client running replica %s' % env['id'])
                    log.info('Environment variables: %s' % env)
                    # run the replica
                    run_process = subprocess.Popen(command, env=env)
                    # now just wait until the job is done
                    run_process.wait()
                    if run_process.poll() != 0:
                        log.error('Replica run returned non-zero code %d!' % run_process.poll())
                elif not manager.active:
                    # otherwise, quit the client only if manager is not active
                    log.error('Client did not get a replica, quitting client!')
                    break
        # END client while loop
    finally:
        # because this is in a try/finally, it will be sure to shutdown the other thread when killed
        log.debug('Stopping manager and joining the manager thread...')
        manager.stop()
        thread.join(1)

    # try connecting to the server to tell it the job is ending
    try:
        server.end_job(options.job_id)
    except ProtocolError:
        log.error('Could not connect to server to notify it of job completion')

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
        # details about mobile server
        self.active = False
        # time this is run! (used in maintenance code)
        self.start_time = datetime.datetime.now()
        # last time a snapshot was made (in seconds since the manager started)
        self.last_snapshot_time = 0
        # snapshot file path
        self.snapshot_path = os.path.join(self.project_path, self.config['manager']['snapshotfile'])
        # host file path
        self.hostfile = os.path.join(self.project_path, self.config['manager']['hostfile'])
        # tell the server to shutdown on maintenance if this is true (THIS IS A HACK)
        self.shutdown = False
        # load the replica selection algorithm class
        log.info('Loading replica selection algorithm class %s...' % self.config['manager']['replica_selection_class'])
        self.rsa_class = globals()[self.config['manager']['replica_selection_class']]()
    
    def _seconds_since_start(self):
        # time since init (how long since the job was started)
        timedelta = datetime.datetime.now() - self.start_time
        # calculate total number of seconds
        return timedelta.seconds + timedelta.days*24*60*60
        
    def stop(self):
        # TODO: this is kind of a hack... there must be a better way. Use signals to kill the manager listener thread?
        log.debug('Stopping manager...')
        self.shutdown = True
        if self.active:
            self.snapshot.save(self)

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
            self.update_replicas_from_config()
            log.info('Snapshot loaded successfuly')
        else:
            # initialize a new snapshot
            self.snapshot = Snapshot(self.snapshot_path)
            # loop through replicas in the config file
            for r_id, r_properties in self.config['replicas'].items():
                log.info('Adding replica: %s -> %s' % (r_id, r_properties))
                # create new Replica objects             
                r = Replica(manager=self, id=r_id, properties=r_properties)
                self.replicas[r.id] = r

        if not len(self.replicas.items()):
            raise Exception('No replicas found in config file... exiting!')
        return True

    def maintain(self):
        """ Maintenance code that runs between daemon.handleRequests() calls """
        if not self.active:
            return True

        # look for any timed-out replicas
        for r_id, r in self.replicas.items():
            if r.status == Replica.RUNNING and datetime.datetime.now() >= r.timeout_time:
                log.error('Replica %s timed-out, ending the run' % r_id)
                r.stop(return_code=1)

        # run autosubmit and snapshot
        if self.config['manager']['autosubmit']:
            self.autosubmit()
        if (self._seconds_since_start()/self.config['manager']['snapshottime']) > (self.last_snapshot_time/self.config['manager']['snapshottime']):
            # write a snapshot
            self.last_snapshot_time = self._seconds_since_start()
            self.snapshot.save(self)

    def autosubmit(self):
        """ Submit replicas if necessary """
        # if necessary submit as many jobs as there are replicas
        running_jobs = [ j for j in self.jobs if not j.completed() ]
        # runnable replicas as those that aren't STOPPED
        runnable_replicas = [ r for r in self.replicas.values() if r.status != Replica.STOPPED ]
        # count how many jobs we need to submit
        new_jobs_needed = len(runnable_replicas) - len(running_jobs)
        # generate a list of new jobs to submit (below)
        jobs_to_submit = [ Job(self) for j in range(new_jobs_needed) ]
        
        # submit a new job every autosubmit_interval seconds (usually every hour)
        try:
            # if we have already submitted jobs, take latest submit time
            last_submit_time = self.jobs[-1].submit_time
        except:
            # by default, submit an extra job
            last_submit_time = 0
        
        # if it's time to submit...        
        if (self._seconds_since_start()/self.config['manager']['autosubmit_interval']) > (last_submit_time/self.config['manager']['autosubmit_interval']):
            jobs_to_submit.append(Job(self))
        
        if len(jobs_to_submit) > 0:
            log.info('Will autosubmit %d new jobs...' % len(jobs_to_submit))
        
        # submit all the jobs that we need to submit
        # be careful though, disable autosubmit on qsub failure
        for j in jobs_to_submit:
            if j.submit():
                self.jobs.append(j)
            else:
                log.error('Job submission failed, disabling autosubmit!')
                self.config['manager']['autosubmit'] = False
                return False
            time.sleep(1) # sleep for 1 second between submits
        return True

    # TODO: maybe also make a dict for this? probably not a big deal at the moment
    def find_job_by_id(self, job_id, create=False):
        for j in self.jobs:
            if str(j.id) == str(job_id):
                return j
        if create:
            log.error('Job with id %s not found, creating new Job' % job_id)                
            job = Job(self)
            job.id = job_id
            self.jobs.append(job)
            return job
        else:
            return None
    
    def update_replicas_from_config(self):
        """ Loop through the replicas in the config and create or update them in the manager """
        for r_id, r_properties in self.config['replicas'].items():
            log.info('Updating replica: %s' % (r_id))
            if r_id not in self.replicas.keys():
                log.info('Replica %s found in config but not in snapshot, adding the replica...' % str(r_id))
                r = Replica(manager=self, id=r_id, properties=r_properties)
                self.replicas[r.id] = r
            else:
                self.replicas[r_id].properties.update(r_properties)
    
    def force_reset(self):
        log.info("Setting all RUNNING replicas to READY...")
        for r in self.replicas.values():
            if r.status == Replica.RUNNING:
                r.stop()
        for j in self.jobs:
            now = datetime.datetime.now()
            if not j.completed():
                try:
                    j.stop_time = now
                except:
                    # TODO: fix this later.. make sure old Job objects can be reset
                    pass
    
    # TODO: rename these functions!
    def get_all_replicas(self):
        pickle_replicas = {}
        for r_id, r in self.replicas.iteritems():
            r_copy = copy.copy(r)
            r_copy.manager = None
            r_copy.daemon = None
            pickle_replicas[r_id] = r_copy
        return pickle_replicas
        
    def get_all_jobs(self):
        pickle_jobs = []
        for j in self.jobs:
            j_copy = copy.copy(j)
            j_copy.manager = None
            pickle_jobs.append(j_copy)
        return pickle_jobs
            
    def set_replica_status(self, replica_id, status):
        try:
            r = self.replicas[replica_id]
            if r.status == Replica.RUNNING:
                log.warning('Could not change replica %s status... replica is running' % str(replica_id))
                return False
            else:
                log.info('Changing replica %s status from %s to %s' % (str(replica_id), r.status, status))
                r.status = status
                return True
        except:
            log.error('Could not change replica %s status' % str(replica_id))
            return False

    #
    # Client calls to Manager
    #
    
    def connect(self, job_id, listener_uri):
        """ Clients make contact with the server by calling this """
        log.info('Job %s connected.' % str(job_id))        
        job = self.find_job_by_id(job_id, create=True)
        # if this is the first time
        if not job.started:
            job.start()
            job.uri = listener_uri
            
            # start the server on this client if it has been more than 59 minutes since we last transferred
            # this is guaranteed to be a new client since the job has just started
            if (datetime.datetime.now()-self.start_time) > datetime.timedelta(minutes=59):
                # save a snapshot first
                self.snapshot.save(self)
                # connect to the listener on the client and start
                log.info('Starting manager on job %s (%s)...' % (str(job_id), listener_uri))
                server_listener = Pyro.core.getProxyForURI(listener_uri)
                server_listener._setTimeout(2)
                try:
                    if server_listener.start():
                        log.debug('Transferred server to job %s' % str(job_id))
                        self.active = False
                except Exception, ex:
                    log.debug('Could not connect to client (%s)' % str(ex))
    
    def get_replica(self, job_id, pbs_nodefile=''):
        """ Get the next replica for a job to run by calling this """
        log.info('Job %s wants a replica' % str(job_id))        
        job = self.find_job_by_id(job_id)
        if not self.active:
            log.error('Server is not active... will not send the client anything')
        elif job is None:
            log.error('Client with invalid job_id (%s) pinged the server!' % (job_id))
        elif job.end_time < datetime.datetime.now() + datetime.timedelta(seconds=float(self.manager.config['job']['replica_walltime'])):
            # see if the remaining walltime < replica walltime (make sure a replica run can finish in time)
            log.warning("Client job doesn't have enough time left to run a replica, will not send one.")
        else:
            # Replica selection algorithm
            r = self.rsa_class.select(self.replicas)
            if r is not None:
                log.info('Sending replica %s to client with job id %s' % (r.id, job.id))
                job.replica_id = r.id
                r.job_id = job.id
                r.start()
                return (r.command(), r.environment_variables(PBS_JOBID=job.id, PBS_NODEFILE=pbs_nodefile))
            else:
                log.warning('No replicas ready to run')
        return None
    
    def end_replica(self, replica_id, job_id, return_code):
        """ Clients call this to clear a replica that finished running """
        log.info('Job %s is clearing replica %s' % (str(job_id), str(replica_id)))        
        job = self.find_job_by_id(job_id)
        if job is None:
            log.error('Job %s is invalid!' % str(job_id))
        elif replica_id not in self.replicas:
            log.error('Job %s trying to clear unknown replica %s!' % (str(job_id), str(replica_id)))
        else:
            return self.replicas[replica_id].stop(return_code)
        return False
        
    def end_job(self, job_id):
        """ Clients call this to end a job that finished running """
        log.info('Job %s is ending' % str(job_id))        
        job = self.find_job_by_id(job_id)
        if job is None:
            log.error('Job %s is invalid!' % str(job_id))
        else:
            job.stop()

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
    STOPPED = 'stopped'         # replica sent an error or has finished

    def __init__(self, manager, id, properties={}):
        Pyro.core.ObjBase.__init__(self)
        self.manager = manager
        self.status = self.READY
        self.start_time = None
        self.timeout_time = None
        # current job running this replica
        self.job_id = None
        # replica properties
        self.properties = properties
        self.id = id
        self.sequence = -1
    
    def __repr__(self):
        return '<Replica %s:%s>' % (str(self.id), self.status)
    
    def start(self):
        """ Start the replica run, set the status to RUNNING """
        self.start_time = datetime.datetime.now()
        self.timeout_time = self.start_time + datetime.timedelta(seconds=float(self.manager.config['job']['replica_walltime']))
        self.status = self.RUNNING
        self.sequence += 1
        log.info('Starting run for replica %s-%s (job %s)' % (str(self.id), str(self.sequence), self.job_id))
    
    def stop(self, return_code=0):
        """ Stop the run """
        # TODO: clean this up
        log.info('Ending run for replica %s-%s (job %s)' % (str(self.id), str(self.sequence), self.job_id))
        if self.status != Replica.RUNNING:
            log.error('Tried to end a replica that was in state: %s... ignoring request!' % self.status)
            return False
        elif return_code != 0:
            log.error('Replica %s-%s returned non-zero code (%s)' % (str(self.id), str(self.sequence), return_code))
            self.status = Replica.STOPPED
        else:
            self.status = self.READY
        self.start_time = None
        self.timeout_time = None
        return True
    
    def environment_variables(self, **kwargs):
        current_properties = self.properties
        for key, value in kwargs.iteritems():
            current_properties.update({key: value})
        current_properties.update({'id': str(self.id), 'sequence': str(self.sequence)})
        return current_properties
    
    def command(self):
        """ Get the actual code that runs the replica on the client node """
        return ['/bin/sh', self.manager.config['job']['run_script']]

class Job(object):
    """
    The Job class represents a job running through the cluster queue system.
    """
        
    DEFAULT_SUBMIT_SCRIPT_TEMPLATE = """
#!/bin/bash
#PBS -l nodes=${nodes}:ppn=${ppn},walltime=${walltime}${pbs_extra}
#PBS -N ${job_name}

# $PBS_O_WORKDIR
# $PBS_JOBID

cd $job_dir

python ${pydr_path} -j $PBS_JOBID

"""

    def __init__(self, manager):
        self.manager = manager
        self.replica_id = None
        self.id = None
        self.uri = None # the URI of the client listener for this job
        self.submit_time = 0
        self.start_time = 0
        self.stop_time = 0
        self.started = False
    
    def job_name(self):
        return self.manager.config['job']['name']
        
    def start(self):
        self.start_time = datetime.datetime.now()
        # end time should be start time + walltime
        self.stop_time = self.start_time + datetime.timedelta(seconds=float(self.manager.config['job']['walltime']))
        self.started = True
        
    def stop(self):
        self.stop_time = datetime.datetime.now()
        
    def make_submit_script(self, options={}):
        """ Generate a submit script from the template """
        
        if os.path.exists(self.manager.config['job']['submit_script']):
            f = open(self.manager.config['job']['submit_script'], 'r')
            s = Template(f.read())
            f.close()
        else:
            s = Template(self.DEFAULT_SUBMIT_SCRIPT_TEMPLATE)
            
        defaults = {    'nodes': self.manager.config['job']['nodes'],
                        'ppn': self.manager.config['job']['ppn'],
                        'walltime': self.manager.config['job']['walltime'],
                        #'pbs_extra': self.manager.config['client']['pbs_extra'],
                        'pbs_extra': ['os=centos53computeA'],
                        'job_name': self.job_name(),
                        'job_dir': self.manager.project_path,
                        'pydr_path': os.path.abspath(os.path.dirname(__file__)),
                    }
        
        defaults.update(options)
            
        if len(defaults['pbs_extra']) > 0:
            defaults['pbs_extra'] = ','+','.join(defaults['pbs_extra'])
        else:
            defaults['pbs_extra'] = ''
        defaults['pydr_path'] = os.path.join(defaults['pydr_path'], 'pydr.py')
        
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
        self.submit_time = datetime.datetime.now()
        # note: client will send the job_id back to server to associate a replica with a job
        qsub_path = self.manager.config['system']['qsub']
        ssh_path = self.manager.config['system']['ssh']
        submit_host = self.manager.config['manager']['submit_host']
        
        # Make sure the temp dir exists.
        # We make a tempdir in the project dir because we need to ssh to a head node to submit, and the script should be available there too
        tmpdir = os.path.join(self.manager.project_path, 'tmp')
        if not os.path.exists(tmpdir):
            os.mkdir(tmpdir)
        
        # create a temporary file in the <project_dir>/tmp
        (fd, f_abspath) = tempfile.mkstemp(dir=tmpdir)
        os.write(fd, self.make_submit_script())
        f_basename = os.path.basename(f_abspath)
        # if the user specified a submit_host then prepare the command
        if submit_host is not None and submit_host != '':
            # ssh gpc01 "cd $PBS_O_WORKDIR; qsub submit.sh"
            submit_command = ' '.join([ssh_path, submit_host, '"cd %s; %s %s"' % (tmpdir, qsub_path, f_basename)])
        else:
            submit_command = ' '.join([qsub_path, f_abspath])
        
        log.debug('Submitting: "%s"' % submit_command)
        process = subprocess.Popen(submit_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        returncode = process.returncode
        (out, err) = process.communicate()
        
        try:
            # use the whole string as the job id
            self.id = out.strip()
            # qsub should return <integer>.<string>
            split_output = self.id.split('.')
            # this will raise an exception if it isnt an integer
            int(split_output[0])
        except Exception, ex:
            log.error('No job_id found in qsub output: %s' % out)
            log.debug('Exception: %s' % str(ex))
            log.debug('Job submit stdout: %s' % out)
            log.debug('Job submit stderr: %s' % err)
            self.id = None
            return False
        else:
            log.info('Job submitted with ID %s' % self.id)
            return True

    def get_job_properties(self):
        if not self.id:
            log.error('Cannot get job properties with unknown job id')
            return None
        
        process = subprocess.Popen('checkjob --format=XML %s' % (self.id,), shell=False, stdout=PIPE, stderr=PIPE)
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
        if not self.started:
            return False
        else:
            return datetime.datetime.now() >= self.stop_time
        
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
            process = subprocess.Popen('qdel %s' % (self.id,), shell=False, stdout=PIPE, stderr=PIPE)
            (out,err) = process.communicate()

#
# Replica Selection Algorithms
#

class RSABase(object):
    """ Base Replica Selection Algorithm (RSA) """
    def __init__(self):
        pass
    
    def select(self, replicas):
        raise NotImplementedError('Select function not implemented in RSA subclass')

class RSARandomReplica(RSABase):
    """ Random replica selection algorithm """
    def select(self, replicas):
        import random
        replica_list = replicas.values()
        random.shuffle(replica_list)
        for r in replica_list:
            if r.status == Replica.READY:
                log.info('RSARandomReplica selected replica: %s' % str(r.id))
                return r
        return None

class RSADistributedReplica(RSABase):
    """ Distributed replica selection algorithm """
    def select(self, replicas):
        return None

#
# Config Specification
#

PYDR_CONFIG_SPEC = """# PyDR Config File

# set a title for this setup
title = string(default='My DR')
# enable log debug mode?
debug = boolean(default=False)

# Paths to system programs
[system]
    qsub = string(default='/opt/torque/bin/qsub')
    # path to ssh, used for job submission when submit_host is not empty (see below)
    ssh = string(default='/usr/bin/ssh')
    # checkjob is unused now, but will be used in the future to check up on jobs
    checkjob = string(default='/usr/local/bin/checkjob')

# Manager (server) settings
[manager]
    port = integer(min=1024, max=65534, default=7766)
    # Name of the file containing the Pyro address of the manager
    hostfile = string(default='hostfile')
    # Automatically submit (qsub) jobs as required when the manager is launched
    autosubmit = boolean(default=True)
    # time between submitting new jobs, by default a new job will be submitted every hour!
    autosubmit_interval = integer(min=3600, max=999999, default=3600)
    # submit host, ssh to this host when running qsub to submit jobs
    submit_host = string(default='gpc01')
    # approximate time interval in seconds for saving snapshots
    snapshottime = integer(min=1, max=999999, default=3600)
    # file name of snapshot file
    snapshotfile = string(default='snapshot.pickle')
    # replica selection algorithm
    replica_selection_class = string(default='RSARandomReplica')
    # mobile server enabled?
    mobile = boolean(default=True)

# Job-specific configuration
[job]
    name = string(default='myjob')
    # PBS submit script options
    # processors per node
    ppn = integer(min=1, max=64, default=1)
    # nodes per job
    nodes = integer(min=1, max=9999999, default=1)
    # walltime in seconds, this will be converted into the PBS submit script walltime format (1:00:00)
    walltime = integer(min=1, max=999999, default=86400)
    # estimated runtime of a single replica run. Manager will reset a replica that has exceeded this walltime
    replica_walltime = integer(min=0, max=999999, default=10000)
    # job submit script
    submit_script = string(default='submit.sh')
    
    # this script is executed by the client for each replica
    # it defaults to run.sh (located in the same directory as config.ini)
    # replica variables are passed to this script via the client
    run_script = string(default='run.sh')

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

# files section will be used in the near future
# [[files]]
    # link files are not modified but are required by each replica/sequence, they are linked to save disk space
    # ex: link = initial.pdb, initial.psf
    # link = string_list(min=0, default=list())
    
    # copy files are files copied to each sequence. They may be modified by the run script at run-time
    # copy = md.conf, tclforces.tcl
    # copy = string_list(min=0, default=list())
    
    # restart files are output files expected after a replica is done running
    # ex: restart = restart.vel, restart.coor, restart.xsc
    # restart = string_list(min=0, default=list())

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
