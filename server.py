#!/usr/bin/python
import optparse
import logging
import datetime
import Pyro.core
import shelve

import pydr

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
    parser.add_option("-t", "--start-time", dest="start_time", default=None, help="Start time in seconds [default: %default]")
    
    (options, args) = parser.parse_args()
    
    config = pydr.setup_config(options.config_file)
    
    # run the Manager in Pyro
    Pyro.core.initServer()
    daemon = Pyro.core.Daemon()
    manager = pydr.Manager(config, daemon)
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
            daemon.handleRequests(5.0)
    finally:
        daemon.shutdown(True)

if __name__=='__main__':
    main()