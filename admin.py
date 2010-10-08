#!/usr/bin/python

import os
import optparse
from configobj import ConfigObj, flatten_errors
from validate import Validator

import Pyro.core
from Pyro.errors import ProtocolError

from pydr import setup_config, Replica, Job, Manager

def main():    
    usage = """
        usage: %prog [options]
    """
    
    parser = optparse.OptionParser(usage)
    parser.add_option("-c", "--config", dest="config_file", default="config.ini", help="Config file [default: %default]")
    # show replicas
    parser.add_option("-l", dest="show_all_replicas", default=False, action="store_true", help="Show all replicas [default: %default]")
    parser.add_option("-s", dest="show_single_replica", default=None, help="Show a single replica [default: %default]")
    # show jobs
    parser.add_option("-j", dest="show_all_jobs", default=False, action="store_true", help="Show all jobs [default: %default]")
    # set replica ready
    parser.add_option("-r", dest="set_replica_ready", default=None, help="Set a replica to READY [default: %default]")
    # set replica stopped
    parser.add_option("-f", dest="set_replica_stopped", default=None, help="Set a replica to STOPPED [default: %default]")
    # force reset
    parser.add_option("--force-reset", dest="force_reset", default=False, action="store_true", help="Force reset of all replicas and jobs in case of a crash [default: %default]")
    parser.add_option("--ready-stopped", dest="ready_stopped", default=False, action="store_true", help="Set all stopped replicas to ready [default: %default]")
        
    (options, args) = parser.parse_args()
    
    config = setup_config(options.config_file, create=False)
    
    project_path = os.path.abspath(os.path.dirname(config.filename))
    hostfile = os.path.join(project_path, config['manager']['hostfile'])
    
    # get the server URI from the hostfile
    if not os.path.exists(hostfile):
        print "No hostfile found at %s, exiting!" % hostfile
        return
    
    f = open(hostfile, 'r')
    server_uri = f.readline()
    f.close()
    
    server = Pyro.core.getProxyForURI(server_uri)
    server._setTimeout(1)
    
    replicas = server.get_all_replicas()
    jobs = server.get_all_jobs()
    
    if options.force_reset:
        print "Resetting all replicas and jobs!!"
        server.force_reset()
        return
    
    if options.ready_stopped:
        print "Setting stopped replicas to ready..."
        for r_id, r in replicas.items():
            if r.status == Replica.STOPPED:
                if not server.set_replica_status(replica_id=r_id, status=Replica.READY):
                    print "Could not change the replica status!"
    
    if options.show_single_replica is not None:
         replica_id = options.show_single_replica
         if replica_id in replicas.keys():
             print  replicas[replica_id] 
    elif options.show_all_replicas:
        for r in replicas.values():
            try:
                job = [ j for j in jobs if j.id == r.job_id ][0]
                job = job.id
            except:
                job = "no job"
            print 'Replica %s:%s -> %s' % (str(r.id), r.status, job)
    
    if options.show_all_jobs:
        for j in jobs:
            if not j.completed():
                if j.replica_id is not None:
                    print "%s -> %s" % (j.id, j.replica_id)
                else:
                    print "%s -> Idle" % (j.id)
        for j in jobs:
            if j.completed():
                print "%s -> Completed" % (j.id)
    
    if options.set_replica_ready is not None:
        replica_id = options.set_replica_ready
        if replica_id in replicas.keys():
            print "Setting replica %s status from %s to ready" % (replica_id, replicas[replica_id].status)
            if not server.set_replica_status(replica_id=replica_id, status=Replica.READY):
                print "Could not change the replica status!"
        else:
            print "Invalid replica id %s" % replica_id
    
    if options.set_replica_stopped is not None:
        replica_id = options.set_replica_stopped
        if replica_id in replicas.keys():
            print "Setting replica %s status from %s to stopped" % (replica_id, replicas[replica_id].status)
            if not server.set_replica_status(replica_id=replica_id, status=Replica.STOPPED):
                print "Could not change the replica status!"
        else:
            print "Invalid replica id %s" % replica_id
    
if __name__=='__main__':
    main()
