#!/usr/bin/python

import os
import optparse
import Pyro.core
# Unused classes must be imported due to pickling
from pydr import setup_config, Replica, Job

def main():    
    usage = """
        usage: %prog [options]
    """
    
    parser = optparse.OptionParser(usage)
    parser.add_option("-c", "--config", dest="config_file", default="config.ini", help="Config file [default: %default]")
    parser.add_option("-q", dest="quiet", default=False, action="store_true", help="Quiet mode [default: %default]")
    # show replicas
    parser.add_option("-l", dest="show_all_replicas", default=False, action="store_true", help="Show all replicas [default: %default]")
    # show jobs
    parser.add_option("-j", dest="show_all_jobs", default=False, action="store_true", help="Show all jobs [default: %default]")
    # set replica ready
    parser.add_option("-r", dest="set_replica_ready", default=None, help="Set a replica to READY [default: %default]")
    # set replica stopped
    parser.add_option("-s", dest="set_replica_stopped", default=None, help="Set a replica to STOPPED [default: %default]")
    # reset stopped jobs
    parser.add_option("--reset-jobs", dest="reset_jobs", default=False, action="store_true", help="Reset stopped jobs [default: %default]")
    # enable autosubmit
    parser.add_option("-a", dest="toggle_autosubmit", default=False, action="store_true", help="Toggle autosubmit [default: %default]")    
    # force reset
    parser.add_option("--force-reset", dest="force_reset", default=False, action="store_true", help="Force reset of all replicas and jobs in case of a crash [default: %default]")
    parser.add_option("--ready-stopped", dest="ready_stopped", default=False, action="store_true", help="Set all stopped replicas to ready [default: %default]")
    
    (options, args) = parser.parse_args()
    
    config = setup_config(options.config_file, create=False)
    
    project_path = os.path.abspath(os.path.dirname(config.filename))
    hostfile = os.path.join(project_path, config['manager']['hostfile'])
    
    # get the server URI from the hostfile
    if not os.path.exists(hostfile):
        if not options.quiet:
            print "No hostfile found at %s, exiting!" % hostfile
        return
    
    print "\nHostfile: %s" % (hostfile)
    f = open(hostfile, 'r')
    server_uri = f.readline()
    f.close()
    
    server = Pyro.core.getProxyForURI(server_uri)
    server._setTimeout(1)

    props = server.get_manager_properties()
    print "\nConnected to: %s server at %s" % (config['title'], server_uri)

    for k,v in props.items():
        print "%s %s" % (k, v)
        
    replicas = server.get_all_replicas()
    jobs = server.get_all_jobs()
    
    replica_groups = {Replica.RUNNING: [], Replica.READY: [], Replica.STOPPED: []}
    for r_id,r in replicas.items():
        replica_groups[r.status].append(r)
    for k,v in replica_groups.items():
        print "%s: %d/%d" % (k.upper(), len(v), len(replicas))

    if options.toggle_autosubmit:
        print "Enabling autosubmit"
        server.toggle_autosubmit()
    
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
    
    if options.reset_jobs:
        print "Resetting stopped jobs..."
        if not server.reset_invalid_jobs():
            print "Could not reset stopped jobs!"
    
    if options.show_all_replicas:
        sorted_items = sorted(replicas.iteritems(), key=lambda (k,v): int(k))
        for k,r in sorted_items:
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

    print ""
    
if __name__=='__main__':
    main()
