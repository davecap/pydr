#!/usr/bin/python
import optparse
import logging
import datetime
import Pyro.core
from Pyro.errors import ProtocolError
import os

from pydr import Snapshot, Replica, Job, setup_config

# setup logging
log = logging.getLogger("pydr-server")
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch = logging.StreamHandler()
log.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
log.addHandler(ch)

def main():
    usage = """
        usage: %prog [options]
    """
    
    parser = optparse.OptionParser(usage)
    parser.add_option("-c", "--config", dest="config_file", default="config.ini", help="Config file [default: %default]")
    parser.add_option("-s", "--snapshot", dest="snapshot_file", default=None, help="Snapshot file [default: %default]")
    parser.add_option("-t", "--start-time", dest="start_time", default=None, help="Job start time in seconds [default: %default]")
    
    (options, args) = parser.parse_args()
    
    config = setup_config(options.config_file, create=True)
    
    # run the Manager in Pyro
    Pyro.core.initServer()
    daemon = Pyro.core.Daemon()
    manager = Manager(config, daemon)
    uri = daemon.connect(manager, 'manager')
    log.info('The daemon is running at: %s:%s' % (daemon.hostname, daemon.port))
    log.info('The manager\'s uri is: %s' % uri)
    
    try:
        # set a fake start time if we are started by a client job
        if options.start_time is None:
            start_time = datetime.datetime.now()
        else:
            start_time = datetime.datetime.fromtimestamp(float(options.start_time))
        
        while 1:
            # run maintenance every 'timedelta' which is the time since starting the cluster job
            manager.maintain(datetime.datetime.now()-start_time)            
            daemon.handleRequests(30.0)
    finally:
        daemon.shutdown(True)

class Manager(Pyro.core.SynchronizedObjBase):
    """
    The Manager class is the class shared by Pyro. It handles jobs, nodes and replicas.
    """

    def __init__(self, config, daemon):
        Pyro.core.SynchronizedObjBase.__init__(self)

        # jobs[<job id>] = Job()
        self.jobs = []
        # replicas[<replica id>] = Replica()
        self.replicas = {}
        self.config = config
        self.last_snapshot_time = 0     # last time a snapshot was made
        # details about mobile server
        self.active = True
        self.server_host = daemon.hostname
        self.server_port = daemon.port

        # write host file :(
        host_file_path = os.path.join(self.project_path(), self.config['server']['hostfile'])
        f = open(host_file_path, 'w')
        log.info('Writing hostfile...')
        f.write('%s:%s' % (daemon.hostname, daemon.port))
        f.close()

        snapshot_path = os.path.join(self.project_path(), self.config['server']['snapshotfile'])

        # load from snapshot if one is found
        if os.path.exists(snapshot_path):
            log.info('Loading snapshot from %s' % (snapshot_path))
            self.snapshot = Snapshot(snapshot_path)
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
        if not self.config['server']['autosubmit']:
            log.info('Autosubmit is off, not submitting replicas')
            return

    def project_path(self):
        return os.path.abspath(os.path.dirname(self.config.filename))

    def maintain(self, timedelta):
        """ Maintenance code that runs between daemon.handleRequests() calls """
        if not self.active:
            log.debug('MAINTENANCE SKIP: server is not active')
            return
        else:
            log.debug('MAINTENANCE: server active!')

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
            self.snapshot.save(self)

        active_jobs = [ j for j in self.jobs if not j.completed() ]
        log.debug('MAINTENANCE: %d available job(s).' % len(active_jobs))
        # free_jobs = [ j for j in self.jobs if j.status == Job.FREE ]
        # log.debug('%d free jobs.' % len(free_jobs))

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
                    client = Pyro.core.getProxyForURI(sorted_jobs[-1].uri)
                    client._setTimeout(2)
                    try:
                        started = client.start_server(self.snapshot.path)
                        if started:
                            log.debug('MAINTENANCE: Started server on client %s' % sorted_jobs[-1].id)
                            self.active = False
                            return
                        else:
                            log.warning('MAINTENANCE: Client (%s) could not start a server' % sorted_jobs[-1].id)
                    except ProtocolError:
                        log.error('MAINTENANCE: Could not connect to youngest client (%s)' % sorted_jobs[-1].id)
                    # pop the last element and try again
                    client._release()
                    sorted_jobs.pop()

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

    # TODO: maybe also make a dict for this? probably not a big deal at the moment
    def find_job_by_id(self, job_id):
        for j in self.jobs:
            if str(j.id) == str(job_id):
                return j
        return None

    # TODO: override this for DR
    def get_next_replica(self):
        """ Get the next replica to run by calling this """
        self.show_replicas()
        for r in self.replicas.values():
            if r.status == Replica.READY:
                log.info('Replica ready: %s' % r)
                return r
        log.warning('No replicas ready to run')
        return None

    #
    # Client calls to Manager
    #

    def get_replica(self, job_id):
        """ Client pings the server when it has nothing to do """
        log.info('Job %s wants a replica' % str(job_id))        
        job = self.find_job_by_id(job_id)
        if job is None:
            log.error('Client with invalid job_id (%s) pinged the server!' % (job_id))
            return
        if self.active:
            # this manager is active... look for a replica
            replica = self.get_next_replica()
            if replica is not None:
                job.replica_id = replica.id
                replica.job_id = job.id
                log.info('Sending replica %s to client with job id %s' % (replica.id, job.id))
                return replica.uri_path
            else:
                log.info('Client pinged but no replicas are ready to run...')
        return None

    def connect(self, job_id, uri):
        """ Clients will FIRST connect to the manager using connect()
            They will send their job_id and a uri to connect back.
        """
        log.info('Job %s connected.' % str(job_id))        
        job = self.find_job_by_id(job_id)
        # TODO: replace with find_or_create_job??
        if job is None:
            log.error('Job sent unknown job id (%s), creating new job' % job_id)                
            job = Job(self)
            job.id = job_id
            self.jobs.append(job)
        # this should be the first time a job is connecting
        log.info('Associating client URI %s with job %s' % (uri, job_id))
        job.reset(uri)

if __name__=='__main__':
    main()